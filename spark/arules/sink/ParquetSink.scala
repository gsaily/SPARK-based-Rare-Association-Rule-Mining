package de.kp.spark.arules.sink
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-ARULES project
* (https://github.com/skrusche63/spark-arules).
* 
* Spark-ARULES is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-ARULES is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-ARULES. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.arules.RequestContext
import de.kp.spark.arules.model._

class ParquetSink(@transient ctx:RequestContext) {

  import ctx.sqlc.createSchemaRDD
  def addRules(req:ServiceRequest,rules:Rules) {
    
    /* url = e.g.../part1/part2/part3/1 */
    val url = req.data(Names.REQ_URL)
   
    val pos = url.lastIndexOf('/')
    
    val base = url.substring(0, pos)
    val step = url.substring(pos+1).toInt + 1
    
    val store = base + "/" + (step + 1)
            
    val table = ctx.sc.parallelize(rules.items.map(x => {
      ParquetRule(x.antecedent,x.consequent,x.support,x.total,x.confidence)	  
    }))
    
    table.saveAsParquetFile(store)  
    
  }

}