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

import net.indigital.util.Properties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.helpers.Loader;

import java.net.URL;
import java.util.List;

/*
 * Main class of this project which includes the main method.
 */
public class mysql2cass {

    private static Logger Log = Logger.getLogger(mysql2cass.class);

    /*
     * At least one argument to pass to the program,
     * which is the path of the config.xml configuration file.
     */
    public static void main(String[] args) {

    	URL url = Loader.getResource("log4j.properties");
    	PropertyConfigurator.configure(url);

        Log.info("Starting mysql2cass...");
        /*
         * At least one argument to pass to the program,
         * which is the configuration file location.
         */
        if (args.length == 2) {
            try {

                /* Checking log variable*/
                Log.info("Trying to load: " + args[1] + " as LOG level.");
                if (args[1].equalsIgnoreCase("DEBUG")) {
                    Log.getRootLogger().setLevel(Level.DEBUG);
                } else if (args[1].equalsIgnoreCase("INFO")) {
                    Log.getRootLogger().setLevel(Level.INFO);
                } else if (args[1].equalsIgnoreCase("WARN")) {
                    Log.getRootLogger().setLevel(Level.WARN);
                } else if (args[1].equalsIgnoreCase("ERROR")) {
                    Log.getRootLogger().setLevel(Level.ERROR);
                } else {
                    Log.error("Found an incorrect LOG_LEVEL. Valid values are DEBUG, INFO, WARN, ERROR." +
                            "Founded:" + args[1]);
                    System.exit(0);
                }

                Log.info("Trying to load: " + args[0]);
                /*
                 * Parsing the configuration file,
                 * For each mapping node of the mapping list,
                 * we create an execution thread and start it,
                 */
                List<Mapping> listMapping = Properties.configure(args[0]);
                for (Mapping map : listMapping) {
                    Log.info("Starting new thread...");
                    Thread tmp = new Thread(map);
                    tmp.setName(tmp.getName() + "___" + map.getID());
                    tmp.start();
                }
            } catch (Exception e) {
                Log.error(e.getMessage(),e);
                System.exit(0);
            }
        } else {
            Log.error("Invalid number of arguments. You might want to run this as:");
            Log.error("java -Dlog4j.logFile=LOG_FILE -jar mysql2cass.jar CONFIG_FILE LOG_LEVEL");
            System.exit(0);
        }
    }
}