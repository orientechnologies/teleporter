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

import org.apache.commons.lang.StringUtils;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.drakkar.factory.OStrategyFactory;
import com.orientechnologies.orient.drakkar.strategies.OImportStrategy;

/**
 * <CLASS DESCRIPTION>
 * 
 * @author Gabriele Ponzi
 * @email  gabriele.ponzi-at-gmaildotcom
 * 
 */

public class OImporter {

  private static final OStrategyFactory factory = new OStrategyFactory();	

  public static void main(String[] args) {

    /*
     * Input args validation
     */

    // Missing argument validation

    if(args.length < 10) {
      OLogManager.instance().error(factory, "%s", "Syntax error, missing argument.");
      OLogManager.instance().error(factory, "%s", "Use: oimport.sh -d <db-driver> -uri <db-uri> -u <username> -pswd <password> -s <chosenStrategy> -ouri <output-orient-db-uri>.");
      exit();
    }
    
    
    // Option args validation

    for(int i=0; i<args.length; i++)

      // syntax check on even index
      
      if(i%2==0) {

        switch(i) {

        case 0: 
          if(!args[0].equals("-d")) {
            OLogManager.instance().error(factory, "Not valid argument %s. Expected argument -d.\n", args[0]);
            exit();
          }

        case 2:
          if(!args[2].equals("-jurl")) {
            OLogManager.instance().error(factory, "Not valid argument %s. Expected argument -uri.\n", args[2]);
            exit();
          }

        case 4: 
          if(!args[4].equals("-u")) {
            OLogManager.instance().error(factory, "Not valid argument %s. Expected argument -u.\n", args[4]);
            exit();
          }

        case 6: 
          if(!args[6].equals("-pswd")) {
            OLogManager.instance().error(factory, "Not valid argument %s. Expected argument -pswd.\n", args[6]);
            exit();
          }

        case 8: 
          if(!args[8].equals("-s")) {
            OLogManager.instance().error(factory, "Not valid argument %s. Expected argument -s.\n", args[8]);
            exit();
          }		
          
        case 10: 
          if(!args[10].equals("-ourl")) {
            OLogManager.instance().error(factory, "Not valid argument %s. Expected argument -ouri.\n", args[8]);
            exit();
          }     

        }
      }

    // syntax check on not-even index (REDO!!)
    
      else {

        switch(i) {

        case 1: 
          if( !args[1].contains(".Driver")) {
            OLogManager.instance().error(factory, "Not valid db-driver name.\n", (Object[])null);
            exit();
          }

        case 3: 
          if( !(StringUtils.countMatches(args[3], ":")>2 && args[3].contains("jdbc:") && args[3].contains("://")) ) {
            OLogManager.instance().error(factory, "Not valid db-uri.\n", (Object[])null);
            exit();
          }

        case 9: 
          if( !(args[9].equals("naive") | args[9].equals("naive-aggregate")) ) {
            OLogManager.instance().error(factory, "Not valid strategy.\n", (Object[])null);
            exit();
          }			
          
        case 11: 
          if( !(args[11].contains("plocal:") | args[11].contains("remote:")) ) {
            OLogManager.instance().error(factory, "Not valid output orient db uri.\n", (Object[])null);
            exit();
          }   
        }
      }

    OImporter.execute(args[1], args[3], args[5], args[7], args[9], args[11]);
  }



  public static void execute(String driver, String uri, String username, String password, String chosenStrategy, String outDbUri) {

    OImportStrategy strategy = factory.buildStrategy(chosenStrategy);
    OLogManager.instance().info(factory, "Chosen strategy: %s\n", chosenStrategy);
    // the last argument represents the nameResolver (non is null)
    strategy.executeStrategy(driver, uri, username, password, outDbUri, null);

  }


  public static void exit() {
    System.exit(0);
  }

}
