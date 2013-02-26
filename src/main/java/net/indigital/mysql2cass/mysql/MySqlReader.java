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

package net.indigital.mysql2cass.mysql;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class MySqlReader {
    private static Logger Log = Logger.getLogger(MySqlReader.class);

    private Connection connect = null;
    private Statement statement = null;
    private ResultSet resultSet = null;

    public Map<String,Map<String,String>> output;
    public Integer lastNumericKey = -1;

    // @TODO Where is my connection pool!? lmartin
    public void connect(String host, Integer port, String db, String user, String pass) throws Exception {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");

            // Setup the connection with the DB
            this.connect = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db +
                    "?" + "user=" + user + "&password=" + pass);

            // Statements allow to issue SQL queries to the database
            this.statement = connect.createStatement();

        } catch (Exception e) {
            this.close();
            //Log.error(e.getMessage(),e);
            throw new RuntimeException(e);
        }
    }

    public void truncateDataBase(String table) throws Exception {
        try {
            Log.info("Truncating table:" + table);

            // Query to select all the rows starting from 'lastNumericKey'
            String query = "TRUNCATE TABLE `"+ table + "`";
            Log.debug(query);

            // We are not going to get any result for this
            this.statement.executeUpdate(query);

        } catch (Exception e) {
            this.close();
            Log.error(e.getMessage(),e);
        }
    }


    public void readDataBase(String table, String numericKeyStr, Integer elementsAtOnce,
                             List<Map<String, String>> maps) throws Exception {
        try {
            this.output = new TreeMap<String, Map<String, String>>();

            // Query to select all the rows starting from 'lastNumericKey'
            String query = "SELECT * FROM "+ table + " WHERE " + table + "." + numericKeyStr + " > " +
                    this.lastNumericKey + " LIMIT 0 , " + elementsAtOnce;
            Log.debug(query);

            // Result set get the result of the SQL query
            this.resultSet = this.statement.executeQuery(query);

            // Logging the maps list.
            Log.debug("Received the MySQL query. Now we get the columns that we really want.");
            while (this.resultSet.next()) {
                Integer integerID = Integer.parseInt(this.resultSet.getString(numericKeyStr));

                Map<String,String> dict = new TreeMap<String,String>();
                for (Map<String, String> m: maps) {
                    String value = this.resultSet.getString(m.get("name"));
                    if (value.length() > 0) {
                        dict.put(m.get("name"), value);
                    }
                }
                /* This 20 characters wide will help us to order the keys later on. */
                output.put(String.format("%020d", integerID), dict);
            }
            if (this.resultSet.last()) {
                this.lastNumericKey = this.resultSet.getInt(numericKeyStr);
            }
        } catch (Exception e) {
            this.close();
            Log.error(e.getMessage(),e);
        }
    }

    // You need to close the resultSet
    public void close() {
        try {
            if (this.resultSet != null) {
                this.resultSet.close();
            }
            if (this.statement != null) {
                this.statement.close();
            }
            if (this.connect != null) {
                this.connect.close();
            }
        } catch (Exception e) {
            Log.error(e.getMessage(),e);
        }
    }
}
