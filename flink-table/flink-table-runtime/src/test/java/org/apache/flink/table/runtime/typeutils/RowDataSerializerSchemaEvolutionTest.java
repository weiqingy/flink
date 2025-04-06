/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the schema evolution capabilities of {@link RowDataSerializer}.
 */
public class RowDataSerializerSchemaEvolutionTest {
    /**
     * Test schema evolution with removed fields (should be incompatible).
     */
    @Test
    public void testSchemaEvolutionWithRemovedField() throws IOException {
        // Original schema: row<name: varchar, age: int, email: varchar>
        RowType originalRowType = RowType.of(
                new LogicalType[] {
                        VarCharType.STRING_TYPE,
                        new IntType(),
                        VarCharType.STRING_TYPE
                },
                new String[] {"name", "age", "email"}
        );

        // New schema with removed field: row<name: varchar, age: int>
        RowType newRowType = RowType.of(
                new LogicalType[] { VarCharType.STRING_TYPE, new IntType() },
                new String[] {"name", "age"}
        );

        assertSchemaCompatibility(originalRowType, newRowType, false);
    }

    /**
     * Test schema evolution with incompatible type changes.
     */
    @Test
    public void testSchemaEvolutionWithIncompatibleType() throws IOException {
        // Original schema: row<name: varchar, age: int>
        RowType originalRowType = RowType.of(
                new LogicalType[] { VarCharType.STRING_TYPE, new IntType() },
                new String[] {"name", "age"}
        );

        // New schema with changed type: row<name: varchar, age: double>
        RowType newRowType = RowType.of(
                new LogicalType[] { VarCharType.STRING_TYPE, new DoubleType() }, // Changed from int to double
                new String[] {"name", "age"}
        );

        assertSchemaCompatibility(originalRowType, newRowType, false);
    }

    /**
     * Test schema evolution with basic field addition (nullable field),
     * including compatibility checking and migration behavior.
     */
    @Test
    public void testSchemaEvolutionWithAddedNullableField() throws IOException {
        // Original schema: row<name: varchar, age: int>
        RowType originalRowType = RowType.of(
                new LogicalType[] { VarCharType.STRING_TYPE, new IntType() },
                new String[] {"name", "age"}
        );

        // New schema: row<name: varchar, age: int, email: varchar(nullable)>
        RowType newRowType = RowType.of(
                new LogicalType[] {
                        VarCharType.STRING_TYPE,
                        new IntType(),
                        new VarCharType(true, VarCharType.MAX_LENGTH)
                },
                new String[] {"name", "age", "email"}
        );

        // Check schema compatibility
        assertSchemaCompatibility(originalRowType, newRowType, true);

        // Create original data
        GenericRowData originalData = new GenericRowData(2);
        originalData.setField(0, StringData.fromString("John Doe"));
        originalData.setField(1, 30);

        // Test both migration methods and verify results
        verifyBothMigrationMethods(originalRowType, originalData, newRowType, "John Doe", 30, null);
    }

    /**
     * Test schema evolution with non-nullable field addition (should be incompatible).
     */
    @Test
    public void testSchemaEvolutionWithAddedNonNullableField() throws IOException {
        // Original schema: row<name: varchar, age: int>
        RowType originalRowType = RowType.of(
                new LogicalType[] { VarCharType.STRING_TYPE, new IntType() },
                new String[] {"name", "age"}
        );

        // New schema with non-nullable field: row<name: varchar, age: int, email: varchar(NON-nullable)>
        RowType newRowType = RowType.of(
                new LogicalType[] {
                        VarCharType.STRING_TYPE,
                        new IntType(),
                        new VarCharType(false, VarCharType.MAX_LENGTH)
                },
                new String[] {"name", "age", "email"}
        );

        assertSchemaCompatibility(originalRowType, newRowType, false);
    }

    /**
     * Test schema evolution compatibility with field reordering (name-based),
     * including migration behavior.
     */
    @Test
    public void testSchemaEvolutionWithReorderedFieldsCompatibility() throws IOException {
        // Original schema: row<name: varchar, age: int, email: varchar>
        RowType originalRowType = RowType.of(
                new LogicalType[] {
                        VarCharType.STRING_TYPE,
                        new IntType(),
                        VarCharType.STRING_TYPE
                },
                new String[] {"name", "age", "email"}
        );

        // New schema with reordered fields: row<age: int, name: varchar, email: varchar>
        RowType newRowType = RowType.of(
                new LogicalType[] {
                        new IntType(),
                        VarCharType.STRING_TYPE,
                        VarCharType.STRING_TYPE
                },
                new String[] {"age", "name", "email"}
        );

        // Check schema compatibility
        assertSchemaCompatibility(originalRowType, newRowType, true);

        // Create original data
        GenericRowData originalData = new GenericRowData(3);
        originalData.setField(0, StringData.fromString("Alice")); // name
        originalData.setField(1, 25);                            // age
        originalData.setField(2, StringData.fromString("alice@example.com")); // email

        // Test migration and verify results (fields should be in the new order)
        verifyBothMigrationMethods(originalRowType, originalData, newRowType,
                25, "Alice", "alice@example.com");
    }

    /**
     * Test schema evolution with multiple added nullable fields combined with reordering.
     */
    @Test
    public void testSchemaEvolutionWithReorderingAddedNullableFields() throws IOException {
        // Original schema: row<name: varchar, age: int>
        RowType originalRowType = RowType.of(
                new LogicalType[] { VarCharType.STRING_TYPE, new IntType() },
                new String[] {"name", "age"}
        );

        // New schema with reordered fields and multiple added nullable fields:
        // row<name: varchar, address: varchar(nullable), age: int, email: varchar(nullable), zipcode: int(nullable)>
        RowType newRowType = RowType.of(
                new LogicalType[] {
                        VarCharType.STRING_TYPE,
                        new VarCharType(true, VarCharType.MAX_LENGTH),
                        new IntType(),
                        new VarCharType(true, VarCharType.MAX_LENGTH),
                        new IntType(true)
                },
                new String[] {"name", "address", "age", "email", "zipcode"}
        );

        // Check schema compatibility
        assertSchemaCompatibility(originalRowType, newRowType, true);

        // Create original data
        GenericRowData originalData = new GenericRowData(2);
        originalData.setField(0, StringData.fromString("Susan"));
        originalData.setField(1, 33);

        // Test both migration methods and verify results
        // All added fields should be null, and age should be in the third position
        verifyBothMigrationMethods(
                originalRowType,
                originalData,
                newRowType,
                "Susan", null, 33, null, null);
    }

    /**
     * Test schema evolution with nested row types where a nullable field is added,
     * including compatibility checking and migration behavior.
     */
    @Test
    public void testSchemaEvolutionWithNestedRowType() throws IOException {
        // Original nested schema: row<header: row<userId: int, timestamp: bigint>>
        RowType originalHeaderType = RowType.of(
                new LogicalType[] { new IntType(), new BigIntType() },
                new String[] {"userId", "timestamp"}
        );
        RowType originalRowType = RowType.of(
                new LogicalType[] { originalHeaderType },
                new String[] {"header"}
        );

        // New nested schema: row<header: row<userId: int, timestamp: bigint, deviceType: varchar(nullable)>>
        RowType newHeaderType = RowType.of(
                new LogicalType[] {
                        new IntType(),
                        new BigIntType(),
                        new VarCharType(true, VarCharType.MAX_LENGTH)
                },
                new String[] {"userId", "timestamp", "deviceType"}
        );
        RowType newRowType = RowType.of(
                new LogicalType[] { newHeaderType },
                new String[] {"header"}
        );

        // Check schema compatibility
        assertSchemaCompatibility(originalRowType, newRowType, true);

        // Create original nested data
        GenericRowData headerRow = new GenericRowData(2);
        headerRow.setField(0, 12345);
        headerRow.setField(1, 1623456789L);

        GenericRowData originalData = new GenericRowData(1);
        originalData.setField(0, headerRow);

        // Test migration and verify nested structure
        verifyNestedRowMigration(
                originalRowType,
                originalData,
                newRowType,
                0,  // header field index
                3,  // new header arity
                12345, 1623456789L, null); // expected header field values
    }

    /**
     * Test schema evolution compatibility with nested row types containing reordered fields and added nullable fields
     * (name-based), including migration behavior.
     */
    @Test
    public void testSchemaEvolutionWithNestedReorderedFieldsCompatibility() throws IOException {
        // Original nested schema:
        // row<header: row<userId: int, timestamp: bigint, deviceType: varchar>>
        RowType originalHeaderType = RowType.of(
                new LogicalType[] { new IntType(), new BigIntType(), VarCharType.STRING_TYPE },
                new String[] {"userId", "timestamp", "deviceType"}
        );
        RowType originalRowType = RowType.of(
                new LogicalType[] { originalHeaderType },
                new String[] {"header"}
        );

        // New nested schema with reordered fields and added nullable fields:
        // row<header: row<deviceType: varchar, location: varchar(nullable), userId: int, timestamp: bigint,
        //             appVersion: varchar(nullable), sessionId: bigint(nullable)>>
        RowType newHeaderType = RowType.of(
                new LogicalType[] {
                        VarCharType.STRING_TYPE,
                        new VarCharType(true, VarCharType.MAX_LENGTH),
                        new IntType(),
                        new BigIntType(),
                        new VarCharType(true, VarCharType.MAX_LENGTH),
                        new BigIntType(true)
                },
                new String[] {"deviceType", "location", "userId", "timestamp", "appVersion", "sessionId"}
        );
        RowType newRowType = RowType.of(
                new LogicalType[] { newHeaderType },
                new String[] {"header"}
        );

        // Check schema compatibility
        assertSchemaCompatibility(originalRowType, newRowType, true);

        // Create original data
        GenericRowData headerData = new GenericRowData(3);
        headerData.setField(0, 987);           // userId
        headerData.setField(1, 1700000000L);   // timestamp
        headerData.setField(2, StringData.fromString("mobile")); // deviceType

        GenericRowData originalData = new GenericRowData(1);
        originalData.setField(0, headerData);

        // Test migration and verify nested structure
        // Fields are reordered and new nullable fields are added
        verifyNestedRowMigration(
                originalRowType,
                originalData,
                newRowType,
                0,  // header field index
                6,  // new header arity (now 6 fields)
                "mobile", null, 987, 1700000000L, null, null); // expected header field values in new order
        // deviceType, location(null), userId, timestamp, appVersion(null), sessionId(null)
    }

    /**
     * Test index-based field matching with reordered types, which should be incompatible
     * when RowType information is not available.
     */
    @Test
    public void testSchemaEvolutionWithIndexBasedReorderedFields() throws IOException {
        // Original types without RowType information
        LogicalType[] originalTypes = new LogicalType[] {
                VarCharType.STRING_TYPE,
                new IntType(),
                new BigIntType()
        };
        LogicalType[] newTypes = new LogicalType[] {
                new IntType(),
                VarCharType.STRING_TYPE,
                new BigIntType()
        };

        // Check schema compatibility (should be incompatible because field positions changed)
        assertSchemaCompatibility(originalTypes, newTypes, false);
    }

    /**
     * Test schema evolution with index-based matching where a non-nullable field is added
     * (should be incompatible).
     */
    @Test
    public void testSchemaEvolutionWithIndexBasedAddedNonNullableField() throws IOException {
        // Original types without RowType information
        LogicalType[] originalTypes = new LogicalType[] {
                VarCharType.STRING_TYPE,
                new IntType()
        };
        LogicalType[] newTypes = new LogicalType[] {
                VarCharType.STRING_TYPE,
                new IntType(),
                new VarCharType(false, VarCharType.MAX_LENGTH)
        };

        // Check schema compatibility (should be incompatible because added field is non-nullable)
        assertSchemaCompatibility(originalTypes, newTypes, false);
    }

    /**
     * Test schema evolution with index-based matching (when RowType information is not available).
     * In this case, field mapping is determined by position rather than by name.
     */
    @Test
    public void testSchemaEvolutionWithIndexBasedMatching() throws IOException {
        // Original types without RowType information
        LogicalType[] originalTypes = new LogicalType[] {
                VarCharType.STRING_TYPE,
                new IntType()
        };

        // New types with added nullable fields (without RowType information)
        LogicalType[] newTypes = new LogicalType[] {
                VarCharType.STRING_TYPE,
                new IntType(),
                new VarCharType(true, VarCharType.MAX_LENGTH),
                new BigIntType(true)
        };

        // Check schema compatibility (should be compatible with migration)
        assertSchemaCompatibility(originalTypes, newTypes, true);

        // Create original data
        GenericRowData originalData = new GenericRowData(2);
        originalData.setField(0, StringData.fromString("Index Based"));
        originalData.setField(1, 42);

        // Test both migration methods and verify results
        verifyIndexBasedMigrationMethods(
                originalTypes,
                originalData,
                newTypes,
                "Index Based", 42, null, null);
    }

    /**
     * Test schema evolution with nested row structures inside an index-based outer schema.
     * This verifies that when the outer level uses index-based matching (LogicalType[] without names),
     * the nested row can still use name-based matching (RowType with field names) for its fields.
     */
    @Test
    public void testSchemaEvolutionWithNestedIndexBasedMatching() throws IOException {
        // Create nested row type with field names
        RowType nestedRowType = RowType.of(
                new LogicalType[] { new IntType(), VarCharType.STRING_TYPE },
                new String[] { "id", "name" }
        );

        // Original types for outer level (index-based, no field names)
        LogicalType[] originalOuterTypes = new LogicalType[] {
                nestedRowType,
                new IntType()
        };

        // Create nested row with added nullable field, but keeping field names
        RowType newNestedRowType = RowType.of(
                new LogicalType[] {
                        new IntType(),
                        VarCharType.STRING_TYPE,
                        new BigIntType(true)
                },
                new String[] { "id", "name", "timestamp" }
        );

        // New types for outer level (index-based, no field names)
        // with a new nullable field added at the end
        LogicalType[] newOuterTypes = new LogicalType[] {
                newNestedRowType,
                new IntType(),
                new VarCharType(true, VarCharType.MAX_LENGTH)
        };

        // Check schema compatibility (should be compatible with migration)
        assertSchemaCompatibility(originalOuterTypes, newOuterTypes, true);

        // Create original nested row data
        GenericRowData nestedRow = new GenericRowData(2);
        nestedRow.setField(0, 123);  // id
        nestedRow.setField(1, StringData.fromString("Mixed Matching"));  // name

        // Create original data
        GenericRowData originalData = new GenericRowData(2);
        originalData.setField(0, nestedRow);  // nested row
        originalData.setField(1, 42);         // outer int field

        // Verify nested row migration with index-based outer structure
        verifyMixedMatchingMigration(
                originalOuterTypes,
                originalData,
                newOuterTypes,
                0,  // nested field index
                3,  // new nested field arity
                123, "Mixed Matching", null);  // expected nested field values
    }

    // ----------------------------------------------------------------------------------------
    //  The following methods are helper methods for testing schema compatibility and migration.
    // ----------------------------------------------------------------------------------------

    /**
     * Performs migration using the migrateState method and returns the deserialized result.
     * This simulates a state backend migration path.
     */
    private RowData migrateUsingStateMethod(
            RowType originalRowType,
            RowData originalData,
            RowType newRowType) throws IOException {

        // Create serializers
        RowDataSerializer originalSerializer = new RowDataSerializer(originalRowType);
        RowDataSerializer newSerializer = new RowDataSerializer(newRowType);
        TypeSerializerSnapshot<RowData> snapshot = originalSerializer.snapshotConfiguration();

        // Serialize original data
        DataOutputSerializer out = new DataOutputSerializer(128);
        originalSerializer.serialize(originalData, out);
        byte[] serializedBytes = out.getCopyOfBuffer();

        // Perform migration
        DataInputDeserializer in = new DataInputDeserializer(serializedBytes);
        DataOutputSerializer migratedOut = new DataOutputSerializer(128);
        snapshot.migrateState(originalSerializer, newSerializer, in, migratedOut);

        // Deserialize migrated data
        byte[] migratedBytes = migratedOut.getCopyOfBuffer();
        DataInputDeserializer migratedIn = new DataInputDeserializer(migratedBytes);
        return newSerializer.deserialize(migratedIn);
    }

    /**
     * Performs migration using the migrateElement method and returns the deserialized result.
     * This simulates direct object migration without serializing first.
     */
    private RowData migrateUsingElementMethod(
            RowType originalRowType,
            RowData originalData,
            RowType newRowType) throws IOException {

        // Create serializers
        RowDataSerializer originalSerializer = new RowDataSerializer(originalRowType);
        RowDataSerializer newSerializer = new RowDataSerializer(newRowType);
        TypeSerializerSnapshot<RowData> snapshot = originalSerializer.snapshotConfiguration();

        // Perform direct element migration
        DataOutputSerializer migratedOut = new DataOutputSerializer(128);
        snapshot.migrateElement(originalSerializer, newSerializer, originalData, migratedOut);

        // Deserialize migrated data
        byte[] migratedBytes = migratedOut.getCopyOfBuffer();
        DataInputDeserializer migratedIn = new DataInputDeserializer(migratedBytes);
        return newSerializer.deserialize(migratedIn);
    }

    /**
     * Helper method for testing schema compatibility.
     * Works for both RowType-based and LogicalType[] array-based serializers.
     */
    private void assertSchemaCompatibility(
            Object originalSchema,
            Object newSchema,
            boolean expectedCompatible) throws IOException {

        // Create serializers
        RowDataSerializer originalSerializer;
        RowDataSerializer newSerializer;

        if (originalSchema instanceof RowType && newSchema instanceof RowType) {
            // RowType-based serializers (name-based matching)
            originalSerializer = new RowDataSerializer((RowType) originalSchema);
            newSerializer = new RowDataSerializer((RowType) newSchema);
        } else if (originalSchema instanceof LogicalType[] && newSchema instanceof LogicalType[]) {
            // LogicalType[]-based serializers (index-based matching)
            originalSerializer = new RowDataSerializer((LogicalType[]) originalSchema);
            newSerializer = new RowDataSerializer((LogicalType[]) newSchema);
        } else {
            throw new IllegalArgumentException("Schemas must be either RowType or LogicalType[]");
        }

        // Create snapshot of the original serializer
        TypeSerializerSnapshot<RowData> snapshot = originalSerializer.snapshotConfiguration();

        // Test compatibility
        TypeSerializerSchemaCompatibility<RowData> compatibility =
                snapshot.resolveSchemaCompatibility(newSerializer);

        if (expectedCompatible) {
            assertThat(compatibility.isCompatibleAfterMigration()).isTrue();
        } else {
            assertThat(compatibility.isIncompatible()).isTrue();
        }
    }

    /**
     * Helper method to verify the expected field values in a RowData object.
     */
    private void verifyRowData(RowData rowData, Object... expectedValues) {
        assertThat(rowData.getArity()).isEqualTo(expectedValues.length);

        for (int i = 0; i < expectedValues.length; i++) {
            if (expectedValues[i] == null) {
                assertThat(rowData.isNullAt(i)).isTrue();
            } else if (expectedValues[i] instanceof String) {
                assertThat(rowData.getString(i).toString()).isEqualTo(expectedValues[i]);
            } else if (expectedValues[i] instanceof Integer) {
                assertThat(rowData.getInt(i)).isEqualTo(expectedValues[i]);
            } else if (expectedValues[i] instanceof Long) {
                assertThat(rowData.getLong(i)).isEqualTo(expectedValues[i]);
            }
        }
    }

    /**
     * Helper method to test both migration methods and verify they produce identical results
     * that match the expected values.
     */
    private void verifyBothMigrationMethods(
            RowType originalRowType,
            RowData originalData,
            RowType newRowType,
            Object... expectedValues) throws IOException {

        // Test migration with migrateState
        RowData migratedState = migrateUsingStateMethod(originalRowType, originalData, newRowType);

        // Test migration with direct element migration
        RowData migratedElement = migrateUsingElementMethod(originalRowType, originalData, newRowType);

        // Verify both results match expected values
        verifyRowData(migratedState, expectedValues);
        verifyRowData(migratedElement, expectedValues);
    }

    /**
     * Helper method to verify nested RowData migration.
     */
    private void verifyNestedRowMigration(
            RowType originalRowType,
            RowData originalData,
            RowType newRowType,
            int nestedField,
            int nestedArity,
            Object... nestedExpectedValues) throws IOException {

        // Test migration with both methods
        RowData migratedState = migrateUsingStateMethod(originalRowType, originalData, newRowType);
        RowData migratedElement = migrateUsingElementMethod(originalRowType, originalData, newRowType);

        // Verify both results
        for (RowData result : new RowData[] { migratedState, migratedElement }) {
            // Verify outer row structure
            assertThat(result.getArity()).isEqualTo(originalData.getArity());

            // Get and verify nested row
            RowData nestedRow = result.getRow(nestedField, nestedArity);
            assertThat(nestedRow).isNotNull();

            // Verify nested values
            verifyRowData(nestedRow, nestedExpectedValues);
        }
    }

    /**
     * Helper method to test both migration methods for index-based matching
     * and verify they produce identical results.
     */
    private void verifyIndexBasedMigrationMethods(
            LogicalType[] originalTypes,
            RowData originalData,
            LogicalType[] newTypes,
            Object... expectedValues) throws IOException {

        // Create serializers without RowType information
        RowDataSerializer originalSerializer = new RowDataSerializer(originalTypes);
        RowDataSerializer newSerializer = new RowDataSerializer(newTypes);

        // Get snapshot
        TypeSerializerSnapshot<RowData> snapshot = originalSerializer.snapshotConfiguration();

        // Test migrateState
        DataOutputSerializer out = new DataOutputSerializer(128);
        originalSerializer.serialize(originalData, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        DataOutputSerializer migratedOut = new DataOutputSerializer(128);

        snapshot.migrateState(originalSerializer, newSerializer, in, migratedOut);

        byte[] migratedBytes = migratedOut.getCopyOfBuffer();
        DataInputDeserializer migratedIn = new DataInputDeserializer(migratedBytes);
        RowData migratedState = newSerializer.deserialize(migratedIn);
        verifyRowData(migratedState, expectedValues); // Verify both results match expected values

        // Test migrateElement
        DataOutputSerializer elementOut = new DataOutputSerializer(128);
        snapshot.migrateElement(originalSerializer, newSerializer, originalData, elementOut);

        DataInputDeserializer elementIn = new DataInputDeserializer(elementOut.getCopyOfBuffer());
        RowData migratedElement = newSerializer.deserialize(elementIn);
        verifyRowData(migratedElement, expectedValues); // Verify both results match expected values
    }

    /**
     * Helper method to verify migration with index-based outer structure and name-based nested rows.
     */
    private void verifyMixedMatchingMigration(
            LogicalType[] originalTypes,
            RowData originalData,
            LogicalType[] newTypes,
            int nestedFieldIndex,
            int nestedFieldArity,
            Object... nestedExpectedValues) throws IOException {

        // Create serializers
        RowDataSerializer originalSerializer = new RowDataSerializer(originalTypes);
        RowDataSerializer newSerializer = new RowDataSerializer(newTypes);
        TypeSerializerSnapshot<RowData> snapshot = originalSerializer.snapshotConfiguration();

        // Test with migrateState
        DataOutputSerializer out = new DataOutputSerializer(128);
        originalSerializer.serialize(originalData, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        DataOutputSerializer migratedOut = new DataOutputSerializer(128);
        snapshot.migrateState(originalSerializer, newSerializer, in, migratedOut);

        DataInputDeserializer migratedIn = new DataInputDeserializer(migratedOut.getCopyOfBuffer());
        RowData migratedState = newSerializer.deserialize(migratedIn);

        // Verify outer structure
        assertThat(migratedState.getArity()).isEqualTo(newTypes.length);

        // Verify the nested row
        RowData nestedRowData = migratedState.getRow(nestedFieldIndex, nestedFieldArity);
        verifyRowData(nestedRowData, nestedExpectedValues);

        // Also test with migrateElement
        DataOutputSerializer elementOut = new DataOutputSerializer(128);
        snapshot.migrateElement(originalSerializer, newSerializer, originalData, elementOut);

        DataInputDeserializer elementIn = new DataInputDeserializer(elementOut.getCopyOfBuffer());
        RowData migratedElement = newSerializer.deserialize(elementIn);

        // Verify the nested row with element migration
        RowData nestedRowDataElement = migratedElement.getRow(nestedFieldIndex, nestedFieldArity);
        verifyRowData(nestedRowDataElement, nestedExpectedValues);
    }
}
