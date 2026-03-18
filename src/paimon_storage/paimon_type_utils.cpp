/*-------------------------------------------------------------------------
 *
 * paimon_type_utils.cpp
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
 *	  src/paimon_storage/paimon_type_utils.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "paimon_type_utils.hpp"

namespace duckdb {

paimon::FieldType PaimonTypeUtils::ConvertFieldType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return paimon::FieldType::BOOLEAN;
	case LogicalTypeId::TINYINT:
		return paimon::FieldType::TINYINT;
	case LogicalTypeId::SMALLINT:
		return paimon::FieldType::SMALLINT;
	case LogicalTypeId::INTEGER:
		return paimon::FieldType::INT;
	case LogicalTypeId::BIGINT:
		return paimon::FieldType::BIGINT;
	case LogicalTypeId::FLOAT:
		return paimon::FieldType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return paimon::FieldType::DOUBLE;
	case LogicalTypeId::VARCHAR:
		return paimon::FieldType::STRING;
	case LogicalTypeId::BLOB:
		return paimon::FieldType::BINARY;
	case LogicalTypeId::TIMESTAMP:
		return paimon::FieldType::TIMESTAMP;
	case LogicalTypeId::DECIMAL:
		return paimon::FieldType::DECIMAL;
	case LogicalTypeId::DATE:
		return paimon::FieldType::DATE;
	default:
		return paimon::FieldType::UNKNOWN;
	}
}

std::optional<paimon::Literal> PaimonTypeUtils::ConvertLiteral(const Value &value, paimon::FieldType field_type) {
	if (value.IsNull()) {
		throw IOException("literal cannot be NULL");
	}

	switch (field_type) {
	case paimon::FieldType::BOOLEAN:
		return paimon::Literal(value.GetValue<bool>());
	case paimon::FieldType::TINYINT:
		return paimon::Literal(value.GetValue<int8_t>());
	case paimon::FieldType::SMALLINT:
		return paimon::Literal(value.GetValue<int16_t>());
	case paimon::FieldType::INT:
		return paimon::Literal(value.GetValue<int32_t>());
	case paimon::FieldType::BIGINT:
		return paimon::Literal(value.GetValue<int64_t>());
	case paimon::FieldType::FLOAT:
		return paimon::Literal(value.GetValue<float>());
	case paimon::FieldType::DOUBLE:
		return paimon::Literal(value.GetValue<double>());
	case paimon::FieldType::STRING: {
		auto str = value.GetValue<string>();
		return paimon::Literal(paimon::FieldType::STRING, str.c_str(), str.size());
	}
	case paimon::FieldType::DATE:
		return paimon::Literal(paimon::FieldType::DATE, value.GetValue<int32_t>());
	default:
		return std::nullopt;
	}
}

} // namespace duckdb
