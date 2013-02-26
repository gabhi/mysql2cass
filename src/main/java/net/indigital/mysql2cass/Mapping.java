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

package net.indigital.mysql2cass;

import net.indigital.mysql2cass.cass.CassWriter;
import net.indigital.mysql2cass.mysql.MySqlReader;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.locks.Lock;


/*
 * This is the main functional class of this program.
 * Each of these Mapping threads will keep the consistency
 * between one MySQL table and its associated Column Family in Cassandra.
 *
 * loop forever:
 *  1) Requests data to MySQL and stores it to a temp structure.
 *  2) Based on the schema, writes the data to Cassandra.
 *
*/
public class Mapping implements Runnable {

    private static Logger Log = Logger.getLogger(Mapping.class);

    public Lock lock;
    public Map<String, Boolean> keyspaces;
    public Boolean truncate;
    public String keysType;
    public Integer pauseMySQLReconnections;
    public Integer pauseCassReconnections;
    public Integer refresh;
    public Integer elementsAtOnce;
    public String mysqlHost;
    public Integer mysqlPort;
    public String db;
    public String user;
    public String pass;
    public String table;
    public String numericKeyStr;
    public List<Map<String, String>> maps;

    public String cassHost;
    public Integer cassPort;

    /*
    * Basic constructor.
    */
    public Mapping( Lock lock,
                    Map<String, Boolean> keyspaces,
                    Boolean truncate,
                    String keysType, Integer pauseMySQLReconnections, Integer pauseCassReconnections,
                    Integer refresh, Integer elementsAtOnce,
                    String mysqlHost, Integer mysqlPort,
                    String db, String user, String pass, String table, String numericKeyStr,
                    List<Map<String, String>> maps,
                    String cassHost, Integer cassPort) {

        this.lock = lock;
        this.keyspaces = keyspaces;
        this.truncate = truncate;
        this.keysType = keysType;
        this.pauseMySQLReconnections = pauseMySQLReconnections;
        this.pauseCassReconnections = pauseCassReconnections;
        this.refresh = refresh;
        this.elementsAtOnce = elementsAtOnce;
        this.mysqlHost = mysqlHost;
        this.mysqlPort = mysqlPort;
        this.db = db;
        this.user = user;
        this.pass = pass;
        this.table = table;
        this.numericKeyStr = numericKeyStr;
        this.maps = maps;
        this.cassHost = cassHost;
        this.cassPort = cassPort;
    }
    public String getID() {
        return this.mysqlHost + ":" +  this.mysqlPort + "__" + this.db + "_" +
                this.table + "__" + this.cassHost + ":" + this.cassPort;
    }

    public void run() {
        Log.info("This thread is assigned to:" +
                " truncate:" + this.truncate +
                " keysType:" + this.keysType +
                ", pauseMySQLReconnections:" + this.pauseMySQLReconnections +
                ", pauseCassReconnections:" + this.pauseCassReconnections +
                ", refresh:" + this.refresh + ", elementsAtOnce:" + this.elementsAtOnce +
                ", mysqlHost:" + this.mysqlHost + ", mysqlPort:" + this.mysqlPort +
                ", db:" + this.db + ", user:" + this.user + ", pass:" + this.pass + ", table:" + this.table +
                ", numericKeyStr:" + this.numericKeyStr + ", maps:" + this.maps +
                ", cassHost:" + this.cassHost + ", cassPort:" + this.cassPort);

        /* Reflects whether the */
        Boolean mysqlSuccess;
        MySqlReader mreader = new MySqlReader();

        /* Reflects whether the set of cassandra cluster/keyspace/schema has successfully been created.
         *  We need to create this just once. (Need to be out of the loop!) */
        Boolean cassCreated;
        Boolean mysqlFirstTime;
        CassWriter cwriter = new CassWriter();


        // Generating a name -> type dict for this schema.
        Map<String, String> nameType = new HashMap<String, String>();
        for (Map<String, String> m: this.maps) {
            nameType.put(m.get("name"), m.get("type"));
        }

        long cassStartTime;
        long cassEndTime;
        long mysqlStartTime;
        long mysqlEndTime;
        long mysqlDiff;
        float mysqlElapsed;
        long cassDiff;
        float cassElapsed;

        String keyspace = this.db;
        String columnFamily = this.table;


        Log.debug("-------------------------------");
        Log.debug("            MySQL init");
        Log.debug("-------------------------------");
        if (this.truncate) {
            Log.debug("MySQL table:" + this.table + " will be truncated.");
            mysqlSuccess = Boolean.FALSE;
            while (!mysqlSuccess) {
                try {
                    /*  MySQL. Making the connection to MySQL. */
                    Log.debug("Setting up mysql connection. host:" + this.mysqlHost + ", port:" + this.mysqlPort);
                    mreader.connect(this.mysqlHost, this.mysqlPort, this.db, this.user, this.pass);

                    /* If the MySQL table needs to be dropped, we will do it just the first time. */
                    mreader.truncateDataBase(this.table);
                    Log.debug("Truncated table:" + this.table);

                    mreader.close();
                    mysqlSuccess = Boolean.TRUE;
                } catch (Exception e) {
                    Log.error(e.getMessage(),e);
                    Log.warn("Error while trying to connect/read to/from mysql host:" + this.mysqlHost +
                            ", port:" + this.mysqlPort + ", table:" + this.table);
                    Log.warn("Not a problem! Retrying the connection within " +
                            this.pauseMySQLReconnections + " ms...");
                     try {
                        Thread.sleep(this.pauseMySQLReconnections);
                     } catch (Exception e2) {
                         Log.error(e2.getMessage(),e2);
                     }
                }
            }
        }  else {
            Log.debug("MySQL table:" + this.table + " doesn't need to be truncated.");
        }

        cassCreated = Boolean.FALSE;
        Log.debug("-------------------------------");
        Log.debug("          CASSANDRA init");
        Log.debug("-------------------------------");
        /* Cassandra creating the schema/columnFamily. Dropping keyspace if needed. */
        while (!cassCreated) {
            try {
                Log.debug("Cassandra cluster/schema/keyspace still not created");
                cwriter.createCluster("mysql2cass_cluster", this.cassHost, this.cassPort);

                this.lock.lock();
                Log.debug("I got the lock");
                try {
                    Log.info("Checking keyspace:" + keyspace + " in keyspaces:" + this.keyspaces.toString());
                    /* Dropping keyspace in case it already exists. */
                    if (!this.keyspaces.containsKey(keyspace)) {
                        Log.info("Since the keyspace:" + keyspace + " hasn't been tried to be dropped, let's try to drop it");
                        cwriter.dropKeyspace(keyspace);
                        this.keyspaces.put(keyspace, Boolean.TRUE);
                        Log.info("Added keyspace:" + keyspace + " to keyspaces:" + this.keyspaces.toString());
                    } else {
                        /* Just one thread has to drop the keyspace in case it already exists. */
                        Log.info("keyspace:" + keyspace + " already tried to be dropped, don't drop it.");
                    }
                } finally {
                    Log.debug("Unlocking the lock");
                    this.lock.unlock();
                }

                cwriter.createKeyspace(keyspace);
                cwriter.createSchema(this.keysType, keyspace, columnFamily, this.maps);
                cassCreated = Boolean.TRUE;
            } catch (Exception e) {
                Log.error(e.getMessage(),e);
                Log.warn("Error while trying to create cluster/schema/keyspace in cass. " +
                        "host:" + this.cassHost + ", port:" + this.cassPort +
                        ", keyspace:" + keyspace + ", columnFamily:" + columnFamily);
                Log.warn("Not a problem! Retrying the connection within " +
                        this.pauseCassReconnections + " ms...");
                try {
                    Thread.sleep(this.pauseCassReconnections);
                } catch (Exception e2) {
                    Log.error(e2.getMessage(),e2);
                }
            }
        }






        /* Run this thread forever, please. */
        for ( ; ; ) {
            try {
                Log.debug("-------------------------------");
                Log.debug("            MySQL");
                Log.debug("-------------------------------");
                mysqlStartTime = new Date().getTime();
                /* MySQL access and reading. */
                mysqlSuccess = Boolean.FALSE;
                while (!mysqlSuccess) {
                    try {
                        /*  MySQL. Making the connection to MySQL. */
                        Log.debug("Setting up mysql connection. host:" + this.mysqlHost + ", port:" + this.mysqlPort);
                        mreader.connect(this.mysqlHost, this.mysqlPort, this.db, this.user, this.pass);

                        /*  MySQL. Getting the elements from MySQL. */
                        Log.debug("Reading elements from host:" + this.mysqlHost + ", port:" + this.mysqlPort +
                                ", table:" + this.table);
                        mreader.readDataBase(this.table, this.numericKeyStr, this.elementsAtOnce, this.maps);
                        /*  MySQL. Closing connections. */
                        Log.debug("Closing connections from host:" + this.mysqlHost + ", port:" + this.mysqlPort +
                                ", table:" + this.table);
                        mreader.close();
                        mysqlSuccess = Boolean.TRUE;
                    } catch (Exception e) {
                        Log.error(e.getMessage(),e);
                        Log.warn("Error while trying to connect/read to/from mysql host:" + this.mysqlHost +
                                ", port:" + this.mysqlPort + ", table:" + this.table);
                        Log.warn("Not a problem! Retrying the connection within " +
                                this.pauseMySQLReconnections + " ms...");
                        Thread.sleep(this.pauseMySQLReconnections);
                    }
                }
                mysqlEndTime = new Date().getTime();

                cassStartTime = new Date().getTime();
                Log.debug("-------------------------------");
                Log.debug("          CASSANDRA");
                Log.debug("-------------------------------");
                /* Cassandra.  Writing. */
                if (mreader.output.size() > 0) {
                    Log.info("We have " + mreader.output.size() + " elements to insert to host:" + this.cassHost
                            + ", port:" + this.cassPort + ", keyspace:" + keyspace + ", column_family:" + columnFamily);

                    /* Cassandra writing. */
                    List<String> keysList = new ArrayList<String>(mreader.output.keySet());
                    while (keysList.size() > 0) {
                        String key = keysList.get(0);
                        Integer integerKey = Integer.parseInt(key);
                        Map<String,String> dict = new HashMap<String,String>();
                        dict = mreader.output.get(key);
                        List<String> keysValuesList = new ArrayList<String>(dict.keySet());
                        Log.debug("Writing [" +  key + "] to cassandra host:" + this.cassHost + ", port:" +
                                this.cassPort + ", keyspace:" + keyspace + ", column_family:" + columnFamily);

                        while (keysValuesList.size() > 0) {
                            String columnName = keysValuesList.get(0);
                            String value = dict.get(columnName).toString();
                            String type = nameType.get(columnName);

                            // Inserting the 'value' in the given 'keyspace', 'columnFamily' and 'key'.
                            // 'type' defines the type for the cassandra columnName type.
                            // Type could be {'string', 'int', 'datetime'}
                            try {
                                cwriter.set(keysType, keyspace, columnFamily, key, columnName, value, type);
                                Log.info("keyspace:" + keyspace + ", columnFamily:" + columnFamily + "[" +
                                        integerKey  + "]" + "[" + columnName + "] = " + value + ". (type=" + type + ")");
                                /* Value successfully inserted. */
                                keysValuesList.remove(0);
                            } catch (Exception e) {
                                //Log.error(e.getMessage(),e);
                                Log.warn("Error while trying to write to cassandra host:" + this.cassHost +
                                        ", port:" + this.cassPort + ", keyspace:" + keyspace +
                                        ", column_family:" + columnFamily + ", columnName:" + columnName +
                                        ", key:" + integerKey + ", value:" + value + ", (type:" + type + ")" );
                                Log.warn("Not a problem! Retrying the connection within " +
                                        this.pauseCassReconnections + " ms...");
                                Thread.sleep(this.pauseCassReconnections);
                            }
                        }
                        Log.info("Just wrote element [" +  integerKey + "] to cassandra host:" + this.cassHost +
                                ", port:" + this.cassPort + ", keyspace:" + keyspace + ", column_family:" + columnFamily);
                        /* Element successfully inserted. */
                        keysList.remove(0);
                    }
                } else {
                    Log.info("Nothing new to write to cassandra host:" + this.cassHost +
                            ", port:" + this.cassPort + ", keyspace:" + keyspace + ", column_family:" + columnFamily +
                            ", numericKeyStr:" + this.numericKeyStr);
                }
                cassEndTime = new Date().getTime();

                /* Giving some information about the timing. */
                if (mreader.output.size() > 0) {
                    mysqlDiff = mysqlEndTime - mysqlStartTime;
                    mysqlElapsed = ((float)mysqlDiff / (float)mreader.output.size());
                    cassDiff = cassEndTime - cassStartTime;
                    cassElapsed = ((float)cassDiff / (float)mreader.output.size());

                    Log.info("Read " + mreader.output.size() +
                            " elements in time:" + mysqlDiff +
                            "ms. time/element:" + mysqlElapsed + "ms.");
                    Log.info("Wrote " + mreader.output.size() +
                            " elements in time:" + cassDiff +
                            "ms. time/element:" + cassElapsed + "ms.");
                }

                /* We don't want to stress the MySQL/cass connections. Let's relax a little bit. */
                Log.info("Sleeping thread for " + this.refresh + "ms.");
                Thread.sleep(this.refresh);
                Log.info("Waking up thread after " + this.refresh + "ms.");


            } catch (Exception e) {
                Log.error(e.getMessage(),e);
            }
        }
    }
}