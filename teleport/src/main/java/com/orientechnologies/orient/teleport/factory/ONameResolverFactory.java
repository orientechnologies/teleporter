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

package com.orientechnologies.orient.teleport.factory;

import com.orientechnologies.orient.teleport.context.OTeleportContext;
import com.orientechnologies.orient.teleport.nameresolver.OJavaConventionNameResolver;
import com.orientechnologies.orient.teleport.nameresolver.ONameResolver;
import com.orientechnologies.orient.teleport.nameresolver.OOriginalConventionNameResolver;

/**
 * Factory used to instantiate a specific NameResolver starting from its name.
 * If the name is not specified (null value) a JavaConventionNameResolver is instantiated.
 *  
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 *
 */

public class ONameResolverFactory {

  public ONameResolver buildNameResolver(String nameResolverConvention, OTeleportContext context) {
    ONameResolver nameResolver = null;


    if(nameResolverConvention == null)  {
      nameResolver = new OOriginalConventionNameResolver();
    }
    else {
      switch(nameResolverConvention) {
      
      case "java": nameResolver = new OJavaConventionNameResolver();
      break;
      
      case "original": nameResolver = new OOriginalConventionNameResolver();
      break;

      default : nameResolver = new OOriginalConventionNameResolver();
        context.getStatistics().warningMessages.add("Name resolver convention '" + nameResolverConvention + "' not found, the original name convention will be adopted!");
        break;

      }
    }
    
    context.setNameResolver(nameResolver);
    
    return nameResolver;
  }


}
