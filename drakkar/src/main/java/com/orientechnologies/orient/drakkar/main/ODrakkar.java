/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.drakkar.main;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.orientechnologies.orient.drakkar.context.ODrakkarContext;
import com.orientechnologies.orient.drakkar.context.OOutputStreamManager;
import com.orientechnologies.orient.drakkar.factory.OStrategyFactory;
import com.orientechnologies.orient.drakkar.strategy.OImportStrategy;
import com.orientechnologies.orient.drakkar.ui.OProgressMonitor;

/**
 * Main Class from which the importing process starts.
 * 
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 * 
 */

public class ODrakkar {

  private static OOutputStreamManager outputManager;

  private static final OStrategyFactory FACTORY = new OStrategyFactory();	


  public static void main(String[] args) {


    // Output Manager setting
    outputManager = new OOutputStreamManager(2);


    /*
     * Input args validation
     */

    // Missing argument validation

    if(args.length < 12) {
      outputManager.error("Syntax error, missing argument.");
      outputManager.error("Use: drakkar.sh -d <db-driver> -jurl <db-uri> -u <username> -pswd <password> -ourl <output-orient-db-uri> -s <chosenStrategy>.");
      exit();
    }


    // Filling the argument map
    Map<String,String> arguments = new HashMap<String,String>();

    int i = 0;
    while(i<args.length) {
      arguments.put(args[i], args[i+1]);
      i += 2;
    }

    // Mandatory args validation

    if(!arguments.containsKey("-d")) {
      outputManager.error("Argument -d is mandatory, please try again with expected argument: -d <your-db-driver-name>\n");
      exit();
    }

    if(!arguments.containsKey("-jurl")) {
      outputManager.error("Argument -jurl is mandatory, please try again with expected argument: -jurl <input-db-jdbc-URL>\n");
      exit();
    }

    if(!arguments.containsKey("-u")) {
      outputManager.error("Argument -u is mandatory, please try again with expected argument: -u <your-db-username>\n");
      exit();
    }

    if(!arguments.containsKey("-pswd")) {
      outputManager.error("Argument -pswd is mandatory, please try again with expected argument: -pswd <your-db-access-password>\n");
      exit();
    }

    if(!arguments.containsKey("-ourl")) {
      outputManager.error("Argument -ourl is mandatory, please try again with expected argument: -ourl <output-orientdb-desired-URL>\n");
      exit();
    }

    if(!arguments.containsKey("-s")) {
      outputManager.error("Argument -s is mandatory, please try again with expected argument: -s <your-chosen-strategy>\n");
      exit();
    }


    // simple syntax check on command

    if(!arguments.get("-d").contains("Driver")) {
      outputManager.error("Not valid db-driver name.\n");
      exit();
    }

    if(!arguments.get("-jurl").contains("jdbc:") && !arguments.get("-jurl").contains("://")) {
      outputManager.error("Not valid db-url.\n");
      exit();
    }

    if(! (arguments.get("-ourl").contains("plocal:") | arguments.get("-ourl").contains("remote:") | arguments.get("-ourl").contains("memory:")) ) {
      outputManager.error("Not valid output orient db uri.\n");
      exit();
    }

    if(! (arguments.get("-s").equals("naive") | arguments.get("-s").equals("naive-aggregate")) ) {
      outputManager.error("Not valid strategy.\n");
      exit();
    }

    if(arguments.get("-v") != null) {
      if(! (arguments.get("-v").equals("0") | arguments.get("-v").equals("1") | arguments.get("-v").equals("2") | arguments.get("-v").equals("3")) ) {
        outputManager.error("Not valid output level. Available levels:\n0 - No messages\n1 - Debug\n2 - Info\n3 - Warning \n");
        exit();
      }
    }

    // Mandatory arguments
    String driver = arguments.get("-d");
    String jurl = arguments.get("-jurl");
    String username = arguments.get("-u");
    String password = arguments.get("-pswd");
    String outDbUrl = arguments.get("-ourl");
    String chosenStrategy = arguments.get("-s");

    // Optional arguments
    String nameResolver = arguments.get("-nr");
    String outputLevel = arguments.get("-v");
    String chosenMapper = "basicDBMapper";  // Mapper argument
    String xmlPath = null;
    if(arguments.containsKey("-hibernate")) {
      chosenMapper = "hibernate";
      xmlPath = arguments.get("-hibernate");
    }
    else if(arguments.containsKey("-jpa")) {
      chosenMapper = "jpa";
      xmlPath = arguments.get("-jpa");
    }

    ODrakkar.execute(driver, jurl, username, password, outDbUrl, chosenStrategy, chosenMapper, xmlPath, nameResolver, outputLevel);
  }

  /**
   * Executes the import of the source DB in a OrientDB Graph through different parameters.
   * 
   * @param driver the driver name of the DBMS from which you want to execute the import
   * @param jurl an absolute URL giving the location of the source DB to import
   * @param username to access to the source DB
   * @param password to access to the source DB
   * @param chosenStrategy the execution approach adopted during the importing of data
   * @param outDbUrl an absolute URI for the destination Orient Graph DB
   * @param nameResolver the name of the resolver which transforms the names of all the elements of the source DB
   *                     according to a specific convention (if null Java convention is adopted)
   * @param outputLevel the level of the logging messages that will be printed on the OutputStream during the execution
   */


  public static void execute(String driver, String jurl, String username, String password, String outDbUrl, String chosenStrategy, String chosenMapper, String xmlPath, String nameResolver, String outputLevel) {


    // OutputStream setting

    if(outputLevel != null)
      outputManager.setLevel(Integer.parseInt(outputLevel));

    // Context and Progress Monitor initialization
    final ODrakkarContext context = new ODrakkarContext();
    context.setOutputManager(outputManager);
    OProgressMonitor progressMonitor = new OProgressMonitor(context);
    progressMonitor.initialize();

    OImportStrategy strategy = FACTORY.buildStrategy(chosenStrategy, context);

    // Timer for statistics notifying
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {

      @Override
      public void run() {
        context.getStatistics().notifyListeners();        
      }
    }, 0, 1000);

    // the last argument represents the nameResolver (non is null)
    strategy.executeStrategy(driver, jurl, username, password, outDbUrl, chosenMapper, xmlPath, nameResolver, context);

    timer.cancel();

  }


  public static void exit() {
    System.exit(0);
  }

}
