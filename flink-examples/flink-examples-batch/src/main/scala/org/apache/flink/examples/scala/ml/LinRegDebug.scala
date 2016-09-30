package org.apache.flink.examples.scala.ml


import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{BLAS, DenseVector}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.optimization.LearningRateMethod
import org.apache.flink.ml.regression.MultipleLinearRegression
/**
  * Created by matefejes on 2016.09.08..
  */
object LinRegDebug {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    //  val trainingDS = env.readTextFile("/home/matefejes/Documents/flink/gitproba/lfink-ml/ipython/linreg_test.csv")
    //    .map(x => x.split(";"))
    //    .map(x => x.toList.map(_.toDouble))
    //    .map { x => LabeledVector(x(2), DenseVector(x.take(2).toArray)) }

    val trainingDS = env.readCsvFile[(String, String, String, String)]("/home/matefejes/Documents/flink/gitproba/flink-ml/ipython/linregtest.csv")
//    ("/home/matefejes/Dropbox/SZTAKI/Flink" +"/testdata/linregtest.csv")
      .map { tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[String].toDouble)
//        println(numList)
        LabeledVector(numList(3), DenseVector(numList.take(3).toArray))
      }
    trainingDS.print()
    //    .map(x => x.toList.map(_.toDouble))
    //    .map { x => LabeledVector(x(3), DenseVector(x.take(3).toArray)) }

    val mlr = MultipleLinearRegression()
     .setIterations(10)
     .setStepsize(1)
     .setConvergenceThreshold(0.001)
     .setLearningRateMethod(LearningRateMethod.Constant)

    mlr.fit(trainingDS)

    val weights = mlr.weightsOption match {
      case Some(weights) => weights.collect()
      case None => throw new Exception("Could not calculate the weights.")
    }

    println(weights)

    //  val predictions = mlr.predict(testingDS)
  }
}
