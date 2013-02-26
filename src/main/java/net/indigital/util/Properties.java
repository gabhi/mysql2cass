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

package net.indigital.util;

import net.indigital.mysql2cass.Mapping;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;


/*
 * This class provides basically an XML parser for the configuration file,
 * generating a Mapping object, where we can easily read the attributes.
*/
public class Properties {
    private static String mapping = "mysql2cass/*";
    private static Logger Log = Logger.getLogger(Properties.class);

    /*
     * This private method reads the XML configuration file
     * and returns the parsed document.
     * This method corresponds to the "syntax" analysis.
    */
    private static Document getDocument(String xmlFileName) {
        Document document = null;
        SAXReader reader = new SAXReader();
        try {
            document = reader.read(xmlFileName);
        } catch (DocumentException e) {
            Log.info("Error reading config file. Check error logs for more info.");
            Log.error(e.getMessage(),e);
            System.exit(0);
        }
        return document;
    }

    /*
     * Basic private method helper.
     * Selects the nodes based on a String.
     */
    private static List<Node> selectNodes(Document doc, String xPath) {
        List<Node> nodes = doc.selectNodes(xPath);
        return nodes;
    }

    /*
     * Main parser function.
     * Easy and big function to parse the configuration file.
     * This method corresponds to the "semantic" analysis.
     *
     * I know this could be much better done! What about generating the XML dtd?
     * @lmartin
     */
    public static List<Mapping> configure(String configFileLocation){
        Document document = getDocument(configFileLocation);
        List<Node> mappingNodes = selectNodes(document, mapping);

        Integer pauseMySQLReconnections; //ms
        Integer pauseCassandraReconnections; //ms
        Integer refresh; //ms
        Integer elementsAtOnce; //num elements

        String mysqlHost;
        Integer mysqlPort;
        String user;
        String pass;
        String db;
        String table;
        String numericKeyStr;
        String cassHost;
        Integer cassPort;
        String keysType;
        Boolean truncateDataBase;

        /* Common to all the mappings. This structure will allow the threads
        *  to know which Cassandra keyspace has already been tried to be removed. */
        Lock lock = new ReentrantLock();
        Map<String, Boolean> keyspaces = new HashMap<String, Boolean>();

        /* All the fields to be mapped. */
        List<Map<String, String>> maps;
        /* A list of parsed Mapping nodes. */
        List<Mapping> listMapping = new ArrayList<Mapping>();

        /*
        * We need to check that we don't have repeated mappings.
        * We based this on calculating an id with the values of the mapping.
        */
        String id;
        List<String> mapsIdList = new ArrayList<String>();

        if (mappingNodes.size() < 1) {
            Log.error("You need at least one mapping node");
            System.exit(0);
        }

        for(Node node : mappingNodes) {
            maps = new ArrayList<Map<String, String>>();

            if((node.getName() == null) ||
               (!(node.getName().equals("mapping"))) ||
               (node.selectSingleNode("@refresh") == null) ||
               (node.selectSingleNode("@refresh").getStringValue().isEmpty()) ||
               (node.selectSingleNode("@elementsAtOnce") == null) ||
               (node.selectSingleNode("@elementsAtOnce").getStringValue().isEmpty()) ) {
                Log.error("Found an incorrect mapping node");
                System.exit(0);
            }
            refresh = Integer.parseInt(node.selectSingleNode("@refresh").getStringValue());
            elementsAtOnce = Integer.parseInt(node.selectSingleNode("@elementsAtOnce").getStringValue());

            List<Node> mysqlNodes = node.selectNodes("mysql");
            List<Node> cassandraNodes = node.selectNodes("cassandra");
            List<Node> mapsNodes = node.selectNodes("maps");
            if ((mysqlNodes.size()       != 1) ||
                (cassandraNodes.size()   != 1) ||
                (mapsNodes.size()        != 1)) {
                Log.error("Each mapping needs one mysql|cassandra|maps node");
                System.exit(0);
            }

            /* Parsing mySQL information */
            Node mysqlNode = (Node) mysqlNodes.toArray()[0];
            if ((mysqlNode.selectSingleNode("@host") == null) ||
                (mysqlNode.selectSingleNode("@host").getStringValue().isEmpty()) ||
                (mysqlNode.selectSingleNode("@user") == null) ||
                (mysqlNode.selectSingleNode("@user").getStringValue().isEmpty()) ||
                (mysqlNode.selectSingleNode("@pass") == null) ||
                (mysqlNode.selectSingleNode("@pass").getStringValue().isEmpty()) ||
                (mysqlNode.selectSingleNode("@db") == null) ||
                (mysqlNode.selectSingleNode("@db").getStringValue().isEmpty()) ||
                (mysqlNode.selectSingleNode("@table") == null) ||
                (mysqlNode.selectSingleNode("@table").getStringValue().isEmpty()) ||
                (mysqlNode.selectSingleNode("@numericKeyStr") == null) ||
                (mysqlNode.selectSingleNode("@numericKeyStr").getStringValue().isEmpty()) ||
                (mysqlNode.selectSingleNode("@truncateDataBase") == null) ||
                (mysqlNode.selectSingleNode("@truncateDataBase").getStringValue().isEmpty()) ||
                (mysqlNode.selectSingleNode("@pauseReconnections") == null) ||
                (mysqlNode.selectSingleNode("@pauseReconnections").getStringValue().isEmpty())) {
                Log.error("Found an incorrect mysql node");
                System.exit(0);
            }
            /* Setting variables for the mySQL parsed information */
            mysqlHost = mysqlNode.selectSingleNode("@host").getStringValue();
            mysqlPort = Integer.parseInt(mysqlNode.selectSingleNode("@port").getStringValue());
            user = mysqlNode.selectSingleNode("@user").getStringValue();
            pass = mysqlNode.selectSingleNode("@pass").getStringValue();
            db = mysqlNode.selectSingleNode("@db").getStringValue();
            table = mysqlNode.selectSingleNode("@table").getStringValue();
            numericKeyStr = mysqlNode.selectSingleNode("@numericKeyStr").getStringValue();

            if ( (!(mysqlNode.selectSingleNode("@truncateDataBase").getStringValue().equals("true"))) &&
                    (!(mysqlNode.selectSingleNode("@truncateDataBase").getStringValue().equals("false"))) ) {
                Log.error("Found an incorrect type in a column: " +
                        mysqlNode.selectSingleNode("@truncateDataBase").getStringValue());
                System.exit(0);
            }

            if (mysqlNode.selectSingleNode("@truncateDataBase").getStringValue().equals("true")) {
                truncateDataBase = Boolean.TRUE;
            } else {
                truncateDataBase = Boolean.FALSE;
            }

            pauseMySQLReconnections =
                    Integer.parseInt(mysqlNode.selectSingleNode("@pauseReconnections").getStringValue());


            /* Parsing cassandra information */
            Node cassandraNode = (Node) cassandraNodes.toArray()[0];
            if ((cassandraNode.selectSingleNode("@host") == null) ||
                (cassandraNode.selectSingleNode("@host").getStringValue().isEmpty()) ||
                (cassandraNode.selectSingleNode("@port") == null) ||
                (cassandraNode.selectSingleNode("@port").getStringValue().isEmpty()) ||
                (cassandraNode.selectSingleNode("@pauseReconnections") == null) ||
                (cassandraNode.selectSingleNode("@pauseReconnections").getStringValue().isEmpty()) ||
                (cassandraNode.selectSingleNode("@keysType") == null) ||
                (cassandraNode.selectSingleNode("@keysType").getStringValue().isEmpty())) {
                Log.error("Found an incorrect cassandra node. Host, port, pauseReconnections or keysType non-set");
                System.exit(0);
            }

            if ((!(cassandraNode.selectSingleNode("@keysType").getStringValue().equals("int"))) &&
                (!(cassandraNode.selectSingleNode("@keysType").getStringValue().equals("string"))) ) {
                Log.error("Found an incorrect type for the Cassandra keys: " +
                    cassandraNode.selectSingleNode("@keysType").getStringValue());
                System.exit(0);
            }

            /* Setting variables for the Cassandra parsed information */
            cassHost = cassandraNode.selectSingleNode("@host").getStringValue();
            cassPort = Integer.parseInt(cassandraNode.selectSingleNode("@port").getStringValue());
            pauseCassandraReconnections = Integer.parseInt(cassandraNode.selectSingleNode("@pauseReconnections").getStringValue());
            keysType = cassandraNode.selectSingleNode("@keysType").getStringValue();

            /* Parsing the columns and adding a 'map' for each column. */
            Node mapsNode = (Node) mapsNodes.toArray()[0];
            List<Node> columnNodes = mapsNode.selectNodes("column");
            if (columnNodes.size() < 2) {
                Log.error("You need at least two columns in the maps");
                System.exit(0);
            }
            for(Node column : columnNodes) {
                if (!(column.getName().equals("column"))) {
                    Log.error("Found an incorrect maps");
                    System.exit(0);
                }

                if ((column.selectSingleNode("@name") == null) ||
                    (column.selectSingleNode("@name").getStringValue().isEmpty()) ||
                    (column.selectSingleNode("@type") == null) ||
                    (column.selectSingleNode("@type").getStringValue().isEmpty()) ||
                    (column.selectSingleNode("@secondaryIndex") == null) ||
                    (column.selectSingleNode("@secondaryIndex").getStringValue().isEmpty())) {
                        Log.error("Found an incorrect column node");
                        System.exit(0);
                }

                if ((!(column.selectSingleNode("@type").getStringValue().equals("int"))) &&
                    (!(column.selectSingleNode("@type").getStringValue().equals("string"))) &&
                    (!(column.selectSingleNode("@type").getStringValue().equals("datetime"))) ) {
                    Log.error("Found an incorrect type in a column: " +
                            column.selectSingleNode("@type").getStringValue());
                    System.exit(0);
                }

                if ( (!(column.selectSingleNode("@secondaryIndex").getStringValue().equals("true"))) &&
                     (!(column.selectSingleNode("@secondaryIndex").getStringValue().equals("false"))) ) {
                    Log.error("Found an incorrect type in a column: " +
                            column.selectSingleNode("@secondaryIndex").getStringValue());
                    System.exit(0);
                }

                Map<String, String> tmp = new HashMap<String, String>();
                tmp.put("name", column.selectSingleNode("@name").getStringValue());
                tmp.put("type", column.selectSingleNode("@type").getStringValue());
                tmp.put("secondaryIndex", column.selectSingleNode("@secondaryIndex").getStringValue());
                maps.add(tmp);

                Log.debug("name:" + column.selectSingleNode("@name").getStringValue() +
                          ", type:" + column.selectSingleNode("@type").getStringValue() +
                          ", secondaryIndex:" + column.selectSingleNode("@secondaryIndex").getStringValue());
            }


            /* Checking if a mapping node similar to the actual has been inserted.*/
            id = mysqlHost + mysqlPort + db + table + cassHost + cassPort;
            if (mapsIdList.contains(id)) {
                Log.error("Found a repeated mapping node with id: " + id);
                System.exit(0);
            } else {
                mapsIdList.add(id);
            }

            /* Finally creating and adding another mapping node. */
            Mapping m = new Mapping(lock, keyspaces, truncateDataBase, keysType, pauseMySQLReconnections, pauseCassandraReconnections, refresh, elementsAtOnce,
                        mysqlHost, mysqlPort, db, user, pass, table, numericKeyStr, maps, cassHost, cassPort);
            listMapping.add(m);
            Log.debug("Success while adding another mapping!");

        }

        /* Finally returning the desired object. */
        return listMapping;
    }
}