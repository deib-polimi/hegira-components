namespace java it.polimi.hegira.models

struct Column {
	1: string columnName,
	2: binary columnValue,
	3: string columnValueType,
	4: optional string timestamp,
	5: bool indexable
}

struct Metamodel {
	1: string partitionGroup,
	2: optional list<string> columnFamilies,
	3: string rowKey,
	4: map<string, list<Column>> columns,
	5: optional i32 actualVdpSize,
}


