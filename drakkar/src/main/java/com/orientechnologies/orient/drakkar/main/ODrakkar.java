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

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.drakkar.factory.OStrategyFactory;
import com.orientechnologies.orient.drakkar.strategy.OImportStrategy;

/**
 * Main Class from which the importing process starts.
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi--at--gmail.com
 * 
 */

public class ODrakkar {

  private static final OStrategyFactory FACTORY = new OStrategyFactory();	
  
  private PrintStream output = System.out;

  public PrintStream getOutput() {
    return output;
  }

  public void setOutput(PrintStream output) {
    this.output = output;
  }

  public static void main(String[] args) {

    /*
     * Input args validation
     */

    // Missing argument validation

    if(args.length < 12) {
      OLogManager.instance().error(FACTORY, "%s", "Syntax error, missing argument.");
      OLogManager.instance().error(FACTORY, "%s", "Use: drakkar.sh -d <db-driver> -jurl <db-uri> -u <username> -pswd <password> -ourl <output-orient-db-uri> -s <chosenStrategy>.");
      exit();
    }
    
    
    // Filling the argument map
    Map<String,String> arguments = new HashMap<String,String>();
    
    int i = 0;
    while(i<args.length) {
      arguments.put(args[i], args[i+1]);
      i += 2;
    }
        
    // Option args validation

    if(!arguments.containsKey("-d")) {
      OLogManager.instance().error(FACTORY, "Argument -d is mandatory, please try again with expected argument: -d <your-db-driver-name>\n", args[0]);
      exit();
    }
    
    if(!arguments.containsKey("-jurl")) {
      OLogManager.instance().error(FACTORY, "Argument -jurl is mandatory, please try again with expected argument: -jurl <input-db-jdbc-URL>\n", args[0]);
      exit();
    }
    
    if(!arguments.containsKey("-u")) {
      OLogManager.instance().error(FACTORY, "Argument -u is mandatory, please try again with expected argument: -u <your-db-username>\n", args[0]);
      exit();
    }
    
    if(!arguments.containsKey("-pswd")) {
      OLogManager.instance().error(FACTORY, "Argument -pswd is mandatory, please try again with expected argument: -pswd <your-db-access-password>\n", args[0]);
      exit();
    }
    
    if(!arguments.containsKey("-ourl")) {
      OLogManager.instance().error(FACTORY, "Argument -ourl is mandatory, please try again with expected argument: -ourl <output-orientdb-desired-URL>\n", args[0]);
      exit();
    }
    
    if(!arguments.containsKey("-s")) {
      OLogManager.instance().error(FACTORY, "Argument -s is mandatory, please try again with expected argument: -s <your-chosen-strategy>\n", args[0]);
      exit();
    }
    
    
    // simple syntax check on command
    
    if(!arguments.get("-d").contains(".Driver")) {
      OLogManager.instance().error(FACTORY, "%s", "Not valid db-driver name.\n");
      exit();
    }
    
    if(!arguments.get("-jurl").contains("jdbc:") && !arguments.get("-jurl").contains("://")) {
      OLogManager.instance().error(FACTORY, "%s", "Not valid db-url.\n");
      exit();
    }
    
    if(! (arguments.get("-ourl").contains("plocal:") | arguments.get("-ourl").contains("remote:") | arguments.get("-ourl").contains("memory:")) ) {
      OLogManager.instance().error(FACTORY, "%s", "Not valid output orient db uri.\n");
      exit();
    }
    
    if(! (arguments.get("-s").equals("naive") | arguments.get("-s").equals("naive-aggregate")) ) {
      OLogManager.instance().error(FACTORY, "%s", "Not valid strategy.\n");
      exit();
    }
   
    
    
    // Mandatory arguments
    String driver = arguments.get("-d");
    String jurl = arguments.get("-jurl");
    String username = arguments.get("-u");
    String password = arguments.get("-pswd");
    String outDbUrl = arguments.get("-ourl");
    String chosenStrategy = arguments.get("-s");
    
    // Discretionary arguments
    String nameResolver = arguments.get("-nr");

    ODrakkar.execute(driver, jurl, username, password, outDbUrl, chosenStrategy, nameResolver);
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
   */


  public static void execute(String driver, String jurl, String username, String password, String outDbUrl, String chosenStrategy, String nameResolver) {

    OImportStrategy strategy = FACTORY.buildStrategy(chosenStrategy);
    
    // the last argument represents the nameResolver (non is null)
    strategy.executeStrategy(driver, jurl, username, password, outDbUrl, nameResolver);

  }


  public static void exit() {
    System.exit(0);
  }

}
