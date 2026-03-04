/*-------------------------------------------------------------------------
 *
 * paimon_scan.cpp
 *
 * Copyright (c) 2026, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *	  src/paimon_storage/paimon_scan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "paimon_catalog.hpp"
#include "paimon_functions.hpp"

#include "paimon/api.h"
#include "paimon/catalog/identifier.h"
#include "paimon/schema/schema.h"

#include <string>

namespace duckdb {

struct PaimonScanBindData : public TableFunctionData {
public:
	string warehouse;
	string dbname;
	string tablename;

	map<string, string> paimon_options;

	ArrowTableSchema arrow_table;
};

static unique_ptr<FunctionData> PaimonScanBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<PaimonScanBindData>();

	string warehouse;
	string dbname;

	if (input.inputs.empty()) {
		throw InvalidInputException("warehouse path is necessary");
	}

	if (input.inputs.size() > 1) {
		bind_data->warehouse = input.inputs[0].ToString();
		bind_data->dbname = input.inputs[1].ToString();
		bind_data->tablename = input.inputs[2].ToString();
	} else {
		auto whole_path = input.inputs[0].ToString();
		auto last_slash = whole_path.find_last_of('/');

		if (last_slash == string::npos) {
			throw InvalidInputException("Invalid database path format: missing '/'");
		}

		bind_data->tablename = whole_path.substr(last_slash + 1);

		whole_path = whole_path.substr(0, last_slash);
		last_slash = whole_path.find_last_of('/');

		string dbname_raw = whole_path.substr(last_slash + 1);
		string dbsuffix(paimon::Catalog::DB_SUFFIX);

		if (!StringUtil::EndsWith(dbname_raw, dbsuffix)) {
			throw InvalidInputException("Invalid database path format");
		}

		bind_data->dbname = dbname_raw.substr(0, dbname_raw.size() - dbsuffix.size());
		bind_data->warehouse = whole_path.substr(0, last_slash);
	}

	bind_data->paimon_options = PaimonCatalog::GetPaimonOptions(context, bind_data->warehouse, {});
	auto paimon_catalog = PaimonCatalog::CreatePaimonCatalog(context, bind_data->warehouse, {});

	auto table_schema_result =
	    paimon_catalog->LoadTableSchema(paimon::Identifier(bind_data->dbname, bind_data->tablename));
	if (!table_schema_result.ok()) {
		throw IOException(table_schema_result.status().ToString());
	}
	auto table_schema = std::move(table_schema_result).value();

	auto arrow_schema_result = table_schema->GetArrowSchema();
	if (!arrow_schema_result.ok()) {
		throw IOException(arrow_schema_result.status().ToString());
	}
	auto arrow_schema = std::move(arrow_schema_result).value();

	// return types and cols
	ArrowTableSchema arrow_table;
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, *arrow_schema);

	names = arrow_table.GetNames();
	return_types = arrow_table.GetTypes();

	bind_data->arrow_table = std::move(arrow_table);

	if (arrow_schema && arrow_schema->release) {
		arrow_schema->release(arrow_schema.get());
	}

	return std::move(bind_data);
}

struct PaimonScanGlobalState : public GlobalTableFunctionState {
	std::vector<std::shared_ptr<paimon::Split>> splits;
	atomic<idx_t> split_index {0};
	string path;
	ArrowTableSchema arrow_table;

	idx_t MaxThreads() const override {
		return splits.size();
	}
};

struct PaimonScanLocalState : public LocalTableFunctionState {
public:
	shared_ptr<ArrowArrayWrapper> chunk;
	std::vector<int> read_column_ids;

	PaimonScanLocalState(PaimonScanGlobalState &gstate, const PaimonScanBindData &bind_data,
	                     vector<column_t> column_ids)
	    : read_column_ids(column_ids.begin(), column_ids.end()), global_state(gstate), bind_data(bind_data) {
		if (NextSplit()) {
			chunk = make_shared_ptr<ArrowArrayWrapper>();
		}
	}

	~PaimonScanLocalState() override {
		ReleaseCurrentBatch();
	}

	paimon::BatchReader::ReadBatch NextBatch() {
		while (!exhausted) {
			auto batch_result = batch_reader->NextBatch();
			if (!batch_result.ok()) {
				throw IOException(batch_result.status().ToString());
			}
			auto batch = std::move(batch_result).value();

			// current split exhausted, try to grab the next one
			if (paimon::BatchReader::IsEofBatch(batch)) {
				// no more splits
				if (!NextSplit()) {
					return paimon::BatchReader::MakeEofBatch();
				}

				continue;
			}

			ReleaseCurrentBatch();
			return batch;
		}

		return paimon::BatchReader::MakeEofBatch();
	}

private:
	unique_ptr<paimon::BatchReader> batch_reader;
	bool exhausted = false;
	PaimonScanGlobalState &global_state;
	const PaimonScanBindData &bind_data;

	//! Release the current batch's arrow data. Must be called before
	//! overwriting arrow_array with a new batch (to avoid leaking the
	//! old one) and in the destructor (before batch_reader is destroyed,
	//! since the release callback may depend on paimon resources).
	void ReleaseCurrentBatch() {
		if (chunk && chunk->arrow_array.release) {
			chunk->arrow_array.release(&chunk->arrow_array);
			chunk->arrow_array.release = nullptr;
		}
	}

	bool NextSplit() {
		// grab the next split from global state and create a new batch_reader
		auto idx = global_state.split_index.fetch_add(1);

		// no more splits are available
		if (idx >= global_state.splits.size()) {
			exhausted = true;
			return false;
		}

		paimon::ReadContextBuilder read_context_builder(global_state.path);
		auto read_context_result =
		    read_context_builder.SetOptions(bind_data.paimon_options).SetReadFieldIds(read_column_ids).Finish();
		if (!read_context_result.ok()) {
			throw IOException(read_context_result.status().ToString());
		}
		auto read_context = std::move(read_context_result).value();

		auto table_read_result = paimon::TableRead::Create(std::move(read_context));
		if (!table_read_result.ok()) {
			throw IOException(table_read_result.status().ToString());
		}
		auto table_read = std::move(table_read_result).value();

		auto batch_reader_result = table_read->CreateReader(global_state.splits[idx]);
		if (!batch_reader_result.ok()) {
			throw IOException(batch_reader_result.status().ToString());
		}
		batch_reader.reset(std::move(batch_reader_result).value().release());

		return true;
	}
};

static unique_ptr<GlobalTableFunctionState> PaimonScanInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<PaimonScanBindData>();
	auto state = make_uniq<PaimonScanGlobalState>();

	auto path = bind.warehouse + "/" + bind.dbname + paimon::Catalog::DB_SUFFIX + "/" + bind.tablename;

	// construct the scanner
	paimon::ScanContextBuilder scan_context_builder(path);

	auto scan_context_result = scan_context_builder.SetOptions(bind.paimon_options).Finish();
	if (!scan_context_result.ok()) {
		throw IOException(scan_context_result.status().ToString());
	}
	auto scan_context = std::move(scan_context_result).value();

	auto scanner_result = paimon::TableScan::Create(std::move(scan_context));
	if (!scanner_result.ok()) {
		throw IOException(scanner_result.status().ToString());
	}
	auto scanner = std::move(scanner_result).value();

	auto plan_result = scanner->CreatePlan();
	if (!plan_result.ok()) {
		throw IOException(plan_result.status().ToString());
	}
	auto plan = std::move(plan_result).value();

	state->splits = plan->Splits();
	state->path = path;
	state->arrow_table = bind.arrow_table;

	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> PaimonScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *gstate) {
	auto &global_state = gstate->Cast<PaimonScanGlobalState>();
	auto &bind = input.bind_data->Cast<PaimonScanBindData>();
	auto local_state = make_uniq<PaimonScanLocalState>(global_state, bind, input.column_ids);

	return std::move(local_state);
}

static void PaimonScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &global_state = input.global_state->Cast<PaimonScanGlobalState>();
	auto &local_state = input.local_state->Cast<PaimonScanLocalState>();

	auto batch = local_state.NextBatch();
	if (paimon::BatchReader::IsEofBatch(batch)) {
		return;
	}

	auto &[c_array, c_schema] = batch;
	local_state.chunk->arrow_array = *c_array;
	c_array->release = nullptr;

	auto output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(c_array->length));
	output.SetCardinality(output_size);

	auto &arrow_types = global_state.arrow_table.GetColumns();

	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		// the first column is ignored
		auto &child_array = *c_array->children[col_idx + 1];
		auto &arrow_type = *arrow_types.at(local_state.read_column_ids[col_idx]);

		ArrowArrayScanState array_state(context);
		array_state.owned_data = local_state.chunk;

		ArrowToDuckDBConversion::SetValidityMask(output.data[col_idx], child_array, 0, output_size,
		                                         local_state.chunk->arrow_array.offset, -1);
		ArrowToDuckDBConversion::ColumnArrowToDuckDB(output.data[col_idx], child_array, 0, array_state, output_size,
		                                             arrow_type);
	}

	if (c_schema && c_schema->release) {
		c_schema->release(c_schema.get());
	}

	output.Verify();
	return;
}

TableFunctionSet PaimonFunctions::GetPaimonScanFunction() {
	TableFunctionSet function_set("paimon_scan");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, PaimonScan,
	                         PaimonScanBind, PaimonScanInitGlobal);
	fun.init_local = PaimonScanInitLocal;
	fun.projection_pushdown = true;
	function_set.AddFunction(fun);

	auto fun_fullpath = TableFunction({LogicalType::VARCHAR}, PaimonScan, PaimonScanBind, PaimonScanInitGlobal);
	fun_fullpath.init_local = PaimonScanInitLocal;
	fun_fullpath.projection_pushdown = true;
	function_set.AddFunction(fun_fullpath);

	return function_set;
}

} // namespace duckdb
