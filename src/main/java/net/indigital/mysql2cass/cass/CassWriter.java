/*
This file is part of mysql2cass.

mysql2cass is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

mysql2cass is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with mysql2cass.  If not, see <http://www.gnu.org/licenses/>.

Author: Luis Martin Gil
        www.luismartingil.com

Contact:
        martingil.luis@gmail.com
        luis.martin.gil@indigital.net

First version: lmartin, July 2012.
*/

package net.indigital.mysql2cass.cass;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;

import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.*;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

import static me.prettyprint.hector.api.factory.HFactory.createColumn;


public class CassWriter {
    private static Logger Log = Logger.getLogger(CassWriter.class);

    private Cluster cluster;

    public void createCluster(String clustername, String address, int port){
        this.cluster= HFactory.getOrCreateCluster(clustername, address + ":" + port);
        Log.info("Cassandra getOrCreateCluster returned. Don't really know if this is good yet.");
    }

    public void dropKeyspace(String keyspaceName) throws Exception {
        if (this.cluster.describeKeyspace(keyspaceName) != null ) {
            Boolean dropped = Boolean.FALSE;
            while (!dropped) {
                try {
                    Log.info("Keyspace:" + keyspaceName + " already exists, dropping it.");
                    cluster.dropKeyspace(keyspaceName, Boolean.TRUE);
                    dropped = Boolean.TRUE;
                } catch (Exception e) {
                    Log.error("Exception while trying to drop the keyspace:" + keyspaceName);
                    Log.error(e.getMessage(),e);
                }
            }
        }
    }

    public void createKeyspace(String keyspaceName) throws Exception {
        int replicationFactor = 1;
        try {
            KeyspaceDefinition keyspacedef = HFactory.createKeyspaceDefinition(keyspaceName,
                    ThriftKsDef.DEF_STRATEGY_CLASS,  replicationFactor, null);
            this.cluster.addKeyspace(keyspacedef, true);
            Log.info("Keyspace:" +  keyspaceName + " created");

        /* Probably keyspace already exists. */
        } catch (HInvalidRequestException e) {
            Log.info("Keyspace:" +  keyspaceName + " already exists. Not a problem.");
            //Log.info(e.getMessage(),e);

        /* Internal Hector exception. Probably host down exception. */
        } catch (me.prettyprint.hector.api.exceptions.HectorException he) {
            Log.info("Found an HectorException. Cluster might be down.");
            //Log.error(he.getMessage(),he);
            throw new RuntimeException(he);

        /* Generic exception. */
        } catch (Exception e) {
            Log.error(e.getMessage(),e);
            throw new RuntimeException(e);
        }
    }

    public void close(){
        this.cluster.getConnectionManager().shutdown();
        Log.info("Cassandra connection closed.");
    }

    public void createSchema (String keysType, String keyspaceName, String columnFamily,
            List<Map<String, String>> maps)  throws Exception {
        final StringSerializer ss = StringSerializer.get();

        try {

            // Creating the column family definition
            BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
            columnFamilyDefinition.setKeyspaceName(keyspaceName);
            columnFamilyDefinition.setName(columnFamily);
            // We don't really need default validation class, but we set it.
            columnFamilyDefinition.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());

            /* According to default behaviour with Cassandra client and CQL client.*/
            columnFamilyDefinition.setReplicateOnWrite(true);
            columnFamilyDefinition.setGcGraceSeconds(864000);
            columnFamilyDefinition.setReadRepairChance(0.1);

            BasicColumnDefinition columnDefinition;
            String name;
            for (Map<String, String> m: maps) {
                name = m.get("name");
                columnDefinition = new BasicColumnDefinition();
                columnDefinition.setName(ss.toByteBuffer(name));

                // Is this column a secondaryIndex?
                if (m.get("secondaryIndex").equals("true")) {
                    // Setting the index as a secondary key.
                    columnDefinition.setIndexType(ColumnIndexType.KEYS);
                    // It is mandatory to set the name for the index as well.The name must be global!
                    columnDefinition.setIndexName(keyspaceName + "__" + columnFamily + "__" + name + "__name");
                }

                // Int type.
                if (m.get("type").equals("int")) {
                    columnDefinition.setValidationClass(ComparatorType.INTEGERTYPE.getClassName());
                // Datetime type.
                } else if (m.get("type").equals("datetime")) {
                    columnDefinition.setValidationClass(ComparatorType.TIMEUUIDTYPE.getClassName());
                // String type.
                } else {
                    columnDefinition.setValidationClass(ComparatorType.UTF8TYPE.getClassName());
                }
                columnFamilyDefinition.addColumnDefinition(columnDefinition);
                Log.info("Added column:" + name + " to the definition of the columnFamily:" + columnFamily);
            }

            ColumnFamilyDefinition cfDef = new ThriftCfDef(columnFamilyDefinition);
            // This will affect to the column names, keep UTF8TYPE
            cfDef.setComparatorType(ComparatorType.UTF8TYPE);

            // The keys will always be INTEGERS but we can write as the user decided (int or string).
            if (keysType.equals("int")) {
                cfDef.setKeyValidationClass(ComparatorType.INTEGERTYPE.getClassName());
            } else {
                cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
            }

            this.cluster.addColumnFamily(cfDef);
            Log.info("columnFamily:" +  columnFamily + " created in the keyspace:" + keyspaceName);

        } catch (Exception e) {
            Log.error(e.getMessage(),e);
            throw new RuntimeException(e);
        }
    }

    // Keys are INTEGERTYPE
    // type could be {'string', 'int', 'datetime'}.
    public void set(String keysType, String keyspaceName, String columnFamily, String key,
                    String columnName, String value, String type) throws Exception {
        final StringSerializer ss = StringSerializer.get();
        final IntegerSerializer si = IntegerSerializer.get();
        final UUIDSerializer su = UUIDSerializer.get();

        Keyspace keyspaceOperator = HFactory.createKeyspace(keyspaceName, this.cluster);

        // The keys will always be INTEGERS but we can write as the user decided (int or string).
        Mutator<Integer> mutatorInt = HFactory.createMutator(keyspaceOperator, si);
        Integer keyInt = Integer.parseInt(key);
        Mutator<String> mutatorStr = HFactory.createMutator(keyspaceOperator, ss);
        String keyStr = String.valueOf(Integer.parseInt(key)); // Doing this conversion not to get the filled size.

        if (type.equals("int")) {
            HColumn<String, Integer> col = createColumn(columnName, Integer.parseInt(value), ss, si);
            Log.debug("Inserting an <int> value. value:" + value);
            /* Must be a better way to do it! DRY! @TODO*/
            if (keysType.equals("int")) {mutatorInt.addInsertion(keyInt, columnFamily, col);
            } else { mutatorStr.addInsertion(keyStr, columnFamily, col);}

        } else if (type.equals("datetime")) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date date = format.parse(value);
            UUID timeUUID = TimeUUIDUtils.getTimeUUID(date.getTime());
            Log.debug("Inserting a <datetime> value. value:" + value + " ,date:" + date +
                    " ,timeUUID:" + timeUUID.toString());
            HColumn<String, UUID> col = createColumn(columnName, timeUUID, ss, su);
            /* Must be a better way to do it! DRY! @TODO*/
            if (keysType.equals("int")) {mutatorInt.addInsertion(keyInt, columnFamily, col);
            } else { mutatorStr.addInsertion(keyStr, columnFamily, col);}
        } else {
            HColumn<String, String> col = createColumn(columnName, value, ss, ss);
            Log.debug("Inserting a <string> value. value:" + value);
            /* Must be a better way to do it! DRY! @TODO*/
            if (keysType.equals("int")) {mutatorInt.addInsertion(keyInt, columnFamily, col);
            } else { mutatorStr.addInsertion(keyStr, columnFamily, col);}
        }

        /* Must be a better way to do it! DRY! */
        if (keysType.equals("int")) {MutationResult mr = mutatorInt.execute();
        } else { MutationResult mr = mutatorStr.execute();}
    }

    /*
    public String get(String keyspaceName, String columnFamily, String key, String columnName) {
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspace);
        columnQuery.setColumnFamily(columnFamily).setKey(key).setName(columnName);
        QueryResult<HColumn<String, String>> result = columnQuery.execute();
        Log.info("Cassandra Result: "+result.get());
        return "test";
    }
    */
}