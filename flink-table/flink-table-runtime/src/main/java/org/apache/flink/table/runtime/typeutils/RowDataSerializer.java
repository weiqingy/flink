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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

/** Serializer for {@link RowData}. */
@Internal
public class RowDataSerializer extends AbstractRowDataSerializer<RowData> {
    private static final long serialVersionUID = 1L;

    private BinaryRowDataSerializer binarySerializer;
    private final LogicalType[] types;
    private final TypeSerializer[] fieldSerializers;
    private final RowData.FieldGetter[] fieldGetters;

    private transient BinaryRowData reuseRow;
    private transient BinaryRowWriter reuseWriter;
    private transient TypeSerializerSnapshot<RowData> cachedSnapshot;
    // Store the original RowType when available
    private final @Nullable RowType originalRowType;

    public RowDataSerializer(RowType rowType) {
        this(
                rowType.getChildren().toArray(new LogicalType[0]),
                rowType.getChildren().stream()
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new),
                rowType);
    }

    public RowDataSerializer(LogicalType... types) {
        this(
                types,
                Arrays.stream(types)
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new),
                null);
    }

    public RowDataSerializer(LogicalType[] types, TypeSerializer<?>[] fieldSerializers) {
        this(types, fieldSerializers, null);
    }

    // Private constructor that takes originalRowType
    private RowDataSerializer(
            LogicalType[] types,
            TypeSerializer<?>[] fieldSerializers,
            @Nullable RowType originalRowType) {
        this.types = types;
        this.fieldSerializers = fieldSerializers;
        this.binarySerializer = new BinaryRowDataSerializer(types.length);
        this.fieldGetters =
                IntStream.range(0, types.length)
                        .mapToObj(i -> RowData.createFieldGetter(types[i], i))
                        .toArray(RowData.FieldGetter[]::new);
        this.originalRowType = originalRowType;
    }

    @Override
    public TypeSerializer<RowData> duplicate() {
        TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
        }
        return new RowDataSerializer(types, duplicateFieldSerializers);
    }

    @Override
    public RowData createInstance() {
        // default use binary row to deserializer
        return new BinaryRowData(types.length);
    }

    @Override
    public void serialize(RowData row, DataOutputView target) throws IOException {
        binarySerializer.serialize(toBinaryRow(row), target);
    }

    @Override
    public RowData deserialize(DataInputView source) throws IOException {
        return binarySerializer.deserialize(source);
    }

    @Override
    public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
        if (reuse instanceof BinaryRowData) {
            return binarySerializer.deserialize((BinaryRowData) reuse, source);
        } else {
            return binarySerializer.deserialize(source);
        }
    }

    @Override
    public RowData copy(RowData from) {
        if (from.getArity() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: " + from.getArity() + ", but serializer arity: " + types.length);
        }
        if (from instanceof BinaryRowData) {
            return ((BinaryRowData) from).copy();
        } else if (from instanceof NestedRowData) {
            return ((NestedRowData) from).copy();
        } else {
            return copyRowData(from, new GenericRowData(from.getArity()));
        }
    }

    @Override
    public RowData copy(RowData from, RowData reuse) {
        if (from.getArity() != types.length || reuse.getArity() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: "
                            + from.getArity()
                            + ", reuse Row arity: "
                            + reuse.getArity()
                            + ", but serializer arity: "
                            + types.length);
        }
        if (from instanceof BinaryRowData) {
            return reuse instanceof BinaryRowData
                    ? ((BinaryRowData) from).copy((BinaryRowData) reuse)
                    : ((BinaryRowData) from).copy();
        } else if (from instanceof NestedRowData) {
            return reuse instanceof NestedRowData
                    ? ((NestedRowData) from).copy(reuse)
                    : ((NestedRowData) from).copy();
        } else {
            return copyRowData(from, reuse);
        }
    }

    @SuppressWarnings("unchecked")
    private RowData copyRowData(RowData from, RowData reuse) {
        GenericRowData ret;
        if (reuse instanceof GenericRowData) {
            ret = (GenericRowData) reuse;
        } else {
            ret = new GenericRowData(from.getArity());
        }
        ret.setRowKind(from.getRowKind());
        for (int i = 0; i < from.getArity(); i++) {
            if (!from.isNullAt(i)) {
                ret.setField(i, fieldSerializers[i].copy((fieldGetters[i].getFieldOrNull(from))));
            } else {
                ret.setField(i, null);
            }
        }
        return ret;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        binarySerializer.copy(source, target);
    }

    @Override
    public int getArity() {
        return types.length;
    }

    /** Convert {@link RowData} into {@link BinaryRowData}. TODO modify it to code gen. */
    @Override
    public BinaryRowData toBinaryRow(RowData row) {
        if (row instanceof BinaryRowData) {
            return (BinaryRowData) row;
        }
        if (reuseRow == null) {
            reuseRow = new BinaryRowData(types.length);
            reuseWriter = new BinaryRowWriter(reuseRow);
        }
        reuseWriter.reset();
        reuseWriter.writeRowKind(row.getRowKind());
        for (int i = 0; i < types.length; i++) {
            if (row.isNullAt(i)) {
                reuseWriter.setNullAt(i);
            } else {
                BinaryWriter.write(
                        reuseWriter,
                        i,
                        fieldGetters[i].getFieldOrNull(row),
                        types[i],
                        fieldSerializers[i]);
            }
        }
        reuseWriter.complete();
        return reuseRow;
    }

    @Override
    public int serializeToPages(RowData row, AbstractPagedOutputView target) throws IOException {
        return binarySerializer.serializeToPages(toBinaryRow(row), target);
    }

    @Override
    public RowData deserializeFromPages(AbstractPagedInputView source) throws IOException {
        throw new UnsupportedOperationException("Not support!");
    }

    @Override
    public RowData deserializeFromPages(RowData reuse, AbstractPagedInputView source)
            throws IOException {
        throw new UnsupportedOperationException("Not support!");
    }

    @Override
    public RowData mapFromPages(RowData reuse, AbstractPagedInputView source) throws IOException {
        if (reuse instanceof BinaryRowData) {
            return binarySerializer.mapFromPages((BinaryRowData) reuse, source);
        } else {
            throw new UnsupportedOperationException("Not support!");
        }
    }

    @Override
    public void skipRecordFromPages(AbstractPagedInputView source) throws IOException {
        binarySerializer.skipRecordFromPages(source);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RowDataSerializer) {
            RowDataSerializer other = (RowDataSerializer) obj;
            return Arrays.equals(fieldSerializers, other.fieldSerializers);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldSerializers);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
        if (cachedSnapshot == null) {
            cachedSnapshot = new RowDataSerializerSnapshot(
                    types, fieldSerializers, originalRowType);
        }
        return cachedSnapshot;
    }

    /** {@link TypeSerializerSnapshot} for {@link RowDataSerializer}. */
    public static final class RowDataSerializerSnapshot implements TypeSerializerSnapshot<RowData> {
        private static final int CURRENT_VERSION = 4;

        private LogicalType[] previousTypes;
        private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;
        private @Nullable RowType originalRowType;

        @SuppressWarnings("unused")
        public RowDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        RowDataSerializerSnapshot(LogicalType[] types, TypeSerializer[] serializers, @Nullable RowType originalRowType) {
            this.previousTypes = types;
            this.nestedSerializersSnapshotDelegate =
                    new NestedSerializersSnapshotDelegate(serializers);
            this.originalRowType = originalRowType;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(previousTypes.length);
            DataOutputViewStream stream = new DataOutputViewStream(out);
            for (LogicalType previousType : previousTypes) {
                InstantiationUtil.serializeObject(stream, previousType);
            }

            // Write whether we have RowType information
            boolean hasRowType = originalRowType != null;
            out.writeBoolean(hasRowType);

            // If we have RowType, serialize it
            if (hasRowType) {
                InstantiationUtil.serializeObject(stream, originalRowType);
            }

            nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            int length = in.readInt();
            DataInputViewStream stream = new DataInputViewStream(in);
            previousTypes = new LogicalType[length];
            for (int i = 0; i < length; i++) {
                try {
                    previousTypes[i] =
                            InstantiationUtil.deserializeObject(stream, userCodeClassLoader);
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            }

            // In version 4+, we added RowType information
            if (readVersion >= 4) {
                boolean hasRowType = in.readBoolean();
                if (hasRowType) {
                    try {
                        originalRowType = InstantiationUtil.deserializeObject(stream, userCodeClassLoader);
                    } catch (ClassNotFoundException e) {
                        throw new IOException(e);
                    }
                } else {
                    originalRowType = null;
                }
            } else {
                // For older versions, no RowType was stored
                originalRowType = null;
            }

            this.nestedSerializersSnapshotDelegate =
                    NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
                            in, userCodeClassLoader);
        }

        @Override
        public RowDataSerializer restoreSerializer() {
            return new RowDataSerializer(
                    previousTypes,
                    nestedSerializersSnapshotDelegate.getRestoredNestedSerializers(),
                    originalRowType);
        }

        @Override
        public TypeSerializerSchemaCompatibility<RowData> resolveSchemaCompatibility(
                TypeSerializer<RowData> newSerializer) {
            if (!(newSerializer instanceof RowDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            RowDataSerializer newRowSerializer = (RowDataSerializer) newSerializer;
            TypeSerializer<?>[] alignedNewSerializers = new TypeSerializer<?>[previousTypes.length];
            boolean requiresMigration = false;

            if (originalRowType != null && newRowSerializer.originalRowType != null) {
                // Use name-based compatibility when both have RowType info
                List<RowType.RowField> oldFields = originalRowType.getFields();
                List<RowType.RowField> newFields = newRowSerializer.originalRowType.getFields();
                int[] oldPosToNewPos = builPosMapping(oldFields, newFields);
                int[] newPosToOldPos = builPosMapping(newFields, oldFields);

                // Check that newly added fields are nullable
                for (int i = 0; i < newPosToOldPos.length; i++) {
                    if (newPosToOldPos[i] == -1 && !newFields.get(i).getType().isNullable()) {
                        return TypeSerializerSchemaCompatibility.incompatible();
                    }
                }

                for (int i = 0; i < oldPosToNewPos.length; i++) {
                    // check if the old field exists in the new schema
                    if (oldPosToNewPos[i] == -1) {
                        return TypeSerializerSchemaCompatibility.incompatible();
                    }
                    int newPos = oldPosToNewPos[i];
                    alignedNewSerializers[i] = newRowSerializer.fieldSerializers[newPos];

                    // Check if field position changed
                    if (newPos != i) {
                        requiresMigration = true;
                    }
                }
                requiresMigration = requiresMigration || newRowSerializer.getArity() > previousTypes.length;
            } else {
                // Fall back to index-based compatibility
                if (previousTypes.length > newRowSerializer.types.length) {
                    return TypeSerializerSchemaCompatibility.incompatible();
                }

                // Check nullability of new fields
                for (int i = previousTypes.length; i < newRowSerializer.types.length; i++) {
                    if (!newRowSerializer.types[i].isNullable()) {
                        return TypeSerializerSchemaCompatibility.incompatible();
                    }
                }
                // Use only common fields for compatibility check
                alignedNewSerializers = Arrays.copyOf(newRowSerializer.fieldSerializers, previousTypes.length);

                // Check compatibility of common fields
                requiresMigration = previousTypes.length < newRowSerializer.types.length;
            }

            // Check compatibility of serializers
            CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData> result =
                    CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                            alignedNewSerializers, nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots());

            return determineCompatibility(result, requiresMigration);
        }

        private TypeSerializerSchemaCompatibility<RowData> determineCompatibility(
            CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData> result, boolean requiresMigration) {
            if (result.isIncompatible()) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            if (result.isCompatibleAfterMigration() || requiresMigration) {
                return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
            }
            if (result.isCompatibleWithReconfiguredSerializer()) {
                return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                        restoreSerializer());
            }
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }

        // Returns -1 for fields in fromFields not present in the toFields.
        private int[] builPosMapping(List<RowType.RowField> fromFields, List<RowType.RowField> toFields) {
            Map<String, Integer> toFieldsMap = new HashMap<>(toFields.size());
            for (int i = 0; i < toFields.size(); i++) {
                toFieldsMap.put(toFields.get(i).getName(), i);
            }

            int[] mapping = new int[fromFields.size()];
            for (int i = 0; i < fromFields.size(); i++) {
                String fromName = fromFields.get(i).getName();
                mapping[i] = toFieldsMap.getOrDefault(fromName, -1);
            }
            return mapping;
        }

        // ---------------------------------------------------------------------------------
        //  The following methods handle state migration when the RowData schema changes.
        // ---------------------------------------------------------------------------------
        @Override
        public void migrateState(
                TypeSerializer<RowData> oldSerializer,
                TypeSerializer<RowData> newSerializer,
                DataInputDeserializer serializedOldValueInput,
                DataOutputSerializer serializedMigratedValueOutput) {
            if (!(newSerializer instanceof RowDataSerializer)) {
                throw new IllegalStateException("Expected RowDataSerializer but got " + newSerializer.getClass().getName());
            }
            try {
                RowDataSerializer oldRowSerializer = (RowDataSerializer) oldSerializer;
                RowDataSerializer newRowSerializer = (RowDataSerializer) newSerializer;
                RowData oldData = oldSerializer.deserialize(serializedOldValueInput);

                // Create and migrate the new row data
                GenericRowData newData = getNewRowData(oldData, oldRowSerializer, newRowSerializer);

                // Serialize the migrated data
                newRowSerializer.serialize(newData, serializedMigratedValueOutput);
            } catch (IOException e) {
                throw new RuntimeException("Error during schema migration", e);
            }
        }

        @Override
        public void migrateElement(
                TypeSerializer<RowData> oldSerializer,
                TypeSerializer<RowData> newSerializer,
                RowData element,
                DataOutputSerializer serializedMigratedValueOutput) throws IOException {
            if (!(newSerializer instanceof RowDataSerializer)) {
                throw new IllegalStateException("Expected RowDataSerializer but got " + newSerializer.getClass().getName());
            }
            try {
                RowDataSerializer oldRowSerializer = (RowDataSerializer) oldSerializer;
                RowDataSerializer newRowSerializer = (RowDataSerializer) newSerializer;

                // Create and migrate the row data directly from the provided element
                GenericRowData newData = getNewRowData(element, oldRowSerializer, newRowSerializer);

                // Serialize the migrated data
                newRowSerializer.serialize(newData, serializedMigratedValueOutput);
            } catch (IOException e) {
                throw new RuntimeException("Error during element migration", e);
            }
        }

        private GenericRowData getNewRowData(
                RowData oldData,
                RowDataSerializer oldSerializer,
                RowDataSerializer newSerializer) {
            GenericRowData newData = new GenericRowData(newSerializer.getArity());
            newData.setRowKind(oldData.getRowKind());

            // Determine mapping strategy and prepare position mappings
            boolean useNameBasedMigration =
                    oldSerializer.originalRowType != null && newSerializer.originalRowType != null;

            int[] positions = new int[newSerializer.getArity()];
            if (useNameBasedMigration) {
                // Name-based field mapping
                List<RowType.RowField> oldFields = oldSerializer.originalRowType.getFields();
                List<RowType.RowField> newFields = newSerializer.originalRowType.getFields();
                positions = builPosMapping(newFields, oldFields);
            } else {
                // Index-based mapping - map positions 1:1 up to the common field count
                int commonFields = Math.min(oldData.getArity(), newSerializer.getArity());
                for (int i = 0; i < newSerializer.getArity(); i++) {
                    positions[i] = i < commonFields ? i : -1;
                }
            }

            // Process all fields using the calculated mapping
            for (int newPos = 0; newPos < newSerializer.getArity(); newPos++) {
                int oldPos = positions[newPos];
                if (oldPos != -1 && !oldData.isNullAt(oldPos)) {
                    Object fieldValue = oldSerializer.fieldGetters[oldPos].getFieldOrNull(oldData);
                    if (fieldValue instanceof RowData) {
                        fieldValue = getNewRowData(
                                (RowData) fieldValue,
                                (RowDataSerializer) oldSerializer.fieldSerializers[oldPos],
                                (RowDataSerializer) newSerializer.fieldSerializers[newPos]);
                    }
                    newData.setField(newPos, fieldValue);
                } else {
                    newData.setField(newPos, null);
                }
            }

            return newData;
        }
    }
}
