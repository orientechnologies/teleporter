package com.orientechnologies.teleporter.mdm;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.teleporter.configuration.api.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Luca Garulli
 */
public class OMDMSerializer extends ORecordSerializerBinary {
  private final OMDMConfiguration configuration;

  public OMDMSerializer(final OMDMConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public String[] getFieldNames(final ODocument reference, final byte[] iSource) {
    final String[] baseFields = super.getFieldNames(reference, iSource);

    final OConfiguredClass config = getConfigurationClass(reference);
    if (config == null)
      return baseFields;

    final Set<String> allFields = new HashSet<String>(config.getConfiguredProperties().size() + baseFields.length);
    for (OConfiguredProperty p : config.getConfiguredProperties()) {
      allFields.add(p.getPropertyName());
    }

    final String[] fieldsAsArray = new String[allFields.size()];
    allFields.toArray(fieldsAsArray);
    return fieldsAsArray;
  }

  /**
   * Strips the external fields.
   */
  @Override
  public byte[] toStream(final ORecord iRecord, final boolean iOnlyDelta) {
    final OConfiguredClass config = getConfigurationClass(iRecord);
    return super.toStream(iRecord, iOnlyDelta);
  }

  /**
   * Includes the external fields.
   */
  @Override
  public ORecord fromStream(final byte[] iSource, final ORecord iRecord, final String[] iFields) {
    ORecord record = super.fromStream(iSource, iRecord, iFields);

    final OConfiguredClass config = getConfigurationClass(iRecord);
    if (config != null) {

      final List<OConfiguredProperty> extProperties = new ArrayList<OConfiguredProperty>();

      final HashSet<String> fields = new HashSet<String>();
      for (String f : iFields)
        fields.add(f);

      for (OConfiguredProperty p : config.getConfiguredProperties()) {
        if (iFields == null || fields.contains(p.getPropertyName()))
          if (!p.isIncludedInMigration())
            extProperties.add(p);
      }

      if (!extProperties.isEmpty()) {
        final OMDMDataSources ds = configuration.getDataSources(ODatabaseRecordThreadLocal.INSTANCE.get().getName());

        final StringBuilder query = new StringBuilder("select ");

        for (int i = 0; i < extProperties.size(); ++i) {
          final OConfiguredProperty p = extProperties.get(i);
          if (i > 0)
            query.append(", ");
          query.append(p.getPropertyName());
        }

        final OSourceTable table = ((OConfiguredVertexClass) config).getMapping().getSourceTables().get(0);
        query.append(" from ");
        query.append(table.getTableName());
        query.append(" where ");

        final String pk = table.getPrimaryKeyColumns().get(0);
        final OConfiguredProperty externalIdField = config.getPropertyByAttribute(pk);

        if (!((ODocument) record).containsField(externalIdField.getPropertyName()))
          // LAZY LOAD THE EXTERNAL ID FIELD
          record = super.fromStream(iSource, iRecord, new String[] { externalIdField.getPropertyName() });

        final Object extId = ((ODocument) record).field(externalIdField.getPropertyName());
        if (extId == null)
          return record;

        query.append(externalIdField.getPropertyName());
        query.append(" = ");

        if (externalIdField.getPropertyType().equalsIgnoreCase("string"))
          query.append("'");

        query.append(extId);

        if (externalIdField.getPropertyType().equals("string"))
          query.append("'");

        final Connection jdbcConnection;
        try {
          jdbcConnection = ds.getConnection(table.getDataSource());

          Statement jdbcStatement = jdbcConnection.createStatement();
          final ResultSet jdbcResultSet = jdbcStatement.executeQuery(query.toString());

          try {
            while (jdbcResultSet.next()) {

              for (int i = 0; i < extProperties.size(); ++i) {
                final OConfiguredProperty prop = extProperties.get(i);
                ((ODocument) record).field(prop.getPropertyName(), jdbcResultSet.getObject(prop.getPropertyName()));
              }
            }

          } finally {
            jdbcResultSet.close();
          }

        } catch (SQLException e) {
          e.printStackTrace();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
    }

    return record;
  }

  private OConfiguredClass getConfigurationClass(final ORecord iRecord) {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null) {
      if (iRecord instanceof ODocument) {
        final OClass cls = ((ODocument) iRecord).getSchemaClass();

        if (cls != null) {
          if (cls.isVertexType())
            return configuration.getVertexClass(db.getName(), ((ODocument) iRecord).getClassName());
          else if (cls.isEdgeType())
            return configuration.getEdgeClass(db.getName(), ((ODocument) iRecord).getClassName());
        }
      }
    }
    return null;
  }

  private OConfiguration getConfiguration(final ORecord iRecord) {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null) {
      if (iRecord instanceof ODocument) {
        final OClass cls = ((ODocument) iRecord).getSchemaClass();

        if (cls != null) {
          if (cls.isVertexType() || cls.isEdgeType())
            return configuration.getEntities(db.getName());
        }
      }
    }
    return null;
  }
}
