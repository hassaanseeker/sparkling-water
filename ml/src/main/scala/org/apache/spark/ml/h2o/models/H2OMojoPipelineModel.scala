package org.apache.spark.ml.h2o.models

import java.io.{File, FileInputStream, InputStream}

import org.apache.spark.h2o.H2OFrame
import org.apache.spark.ml.util.Identifiable

class H2OMojoPipelineModel {


  def predict(h2OFrame: H2OFrame): H2OFrame = {
    
  }
}

object H2OMojoPipelineModel {

  def createFromMojoPipeline(path: String): H2OMojoPipelineModel = {
    val f = new File(path)
    createFromMojoPipeline(new FileInputStream(f), f.getName)
  }

  def createFromMojoPipeline(is: InputStream, uid: String = Identifiable.randomUID("mojoPipeline")): H2OMojoPipelineModel = {
    null
  }
}