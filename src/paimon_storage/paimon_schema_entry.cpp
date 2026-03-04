/*-------------------------------------------------------------------------
 *
 * paimon_schema_entry.cpp
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
 *	  src/paimon_storage/paimon_schema_entry.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "paimon_catalog.hpp"
#include "paimon_schema_entry.hpp"
#include "paimon_table_set.hpp"

namespace duckdb {

PaimonSchemaEntry::PaimonSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this) {
}

static bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return true;
	default:
		return false;
	}
}

void PaimonSchemaEntry::Scan(ClientContext &context, CatalogType type,
                             const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}

	auto &tables = GetTables();
	tables.Scan(context, callback);
}

void PaimonSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without ClientContext not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                          const EntryLookupInfo &lookup_info) {
	if (!CatalogTypeIsSupported(lookup_info.GetCatalogType())) {
		return nullptr;
	}

	auto &tables = GetTables();
	return tables.GetEntry(transaction.GetContext(), lookup_info);
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateIndex(CatalogTransaction, CreateIndexInfo &, TableCatalogEntry &) {
	throw NotImplementedException("CreateIndex not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
	throw NotImplementedException("CreateFunction not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTable(CatalogTransaction, BoundCreateTableInfo &) {
	throw NotImplementedException("CreateTable not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateView(CatalogTransaction, CreateViewInfo &) {
	throw NotImplementedException("CreateView not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	throw NotImplementedException("CreateSequence not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
	throw NotImplementedException("CreateTableFunction not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
	throw NotImplementedException("CreateCopyFunction not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
	throw NotImplementedException("CreatePragmaFunction not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
	throw NotImplementedException("CreateCollation not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateType(CatalogTransaction, CreateTypeInfo &) {
	throw NotImplementedException("CreateType not supported yet");
}

void PaimonSchemaEntry::DropEntry(ClientContext &, DropInfo &) {
	throw NotImplementedException("DropEntry not supported yet");
}

void PaimonSchemaEntry::Alter(CatalogTransaction, AlterInfo &) {
	throw NotImplementedException("Alter not supported yet");
}

} // namespace duckdb
