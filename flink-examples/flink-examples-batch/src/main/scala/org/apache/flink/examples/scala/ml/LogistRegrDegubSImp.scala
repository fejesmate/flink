/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.examples.scala.ml

import breeze.numerics._
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{BLAS, DenseVector}
import org.apache.flink.ml.optimization.LearningRateMethod


/**
  * Created by User on 2016. 07. 04..
  */
object LogistRegrDegubSImp {
  def main(args: Array[String]) {
    val vmi = BLAS.logist(4.0)
    println(vmi)


    //final ParameterTool params = ParameterTool.fromArgs(args);
    //val vmi= org.apache.flink.ml.math.BLAS
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    println(vmi)
    //
    //    val text = env.fromElements(
    //      "Who's there?",
    //      "I think I hear them. Stand, ho! Who's there?")

    //
    //    val trainingDS = env.readTextFile("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\otp_ru_2014-15_onehot_szep_mod1_tan.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(42),DenseVector(x.take(42).toArray))}
    //
    //    val testingDS = env.readTextFile("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\otp_ru_2014-15_onehot_szep_mod1_test.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(42),DenseVector(x.take(42).toArray))}
    //      .map(_.vector)

    //    val trainingDS = env.readTextFile("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\mlogr02_tn.csv")
    //      .map(x => x.split(","))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(3)-1,DenseVector(x.take(3).toArray))} //42
    //
    //    //trainingDS.writeAsText("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\res0.data")
    //
    //    val testingDS = env.readTextFile("C:/Users/User/Dropbox/SZTAKI/Flink
    // /testdata/mlogr02_tn.csv")
    //      .map(x => x.split(","))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(3)-1,DenseVector(x.take(3).toArray))}
    //      //.map(_.vector)

    //    val trainingDS = env.readTextFile("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\logregproba_3dim_norm_mix_elbaszott.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(3),DenseVector(x.take(3).toArray))} //42
    //
    //    //trainingDS.writeAsText("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\res0.data")
    //
    //    val testingDS = env.readTextFile("C:/Users/User/Dropbox/SZTAKI/Flink
    // /testdata/logregproba_3dim_norm_mix_elbaszott.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(3),DenseVector(x.take(3).toArray))}
    //    //.map(_.vector)

//    val trainingDS = env.readTextFile("/home/matefejes/Dropbox/SZTAKI/Flink/testdata/selfmade/logregdataLEARN.csv")
//      .map(x => x.split(","))
//      .map(x => x.toList.map(_.toDouble))
//      .map { x => LabeledVector(x(2), DenseVector(x.take(2).toArray)) } //42
//
//    //trainingDS.writeAsText("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
//    // \\testdata\\res0.data")
//
//    val testingDS = env.readTextFile("/home/matefejes/Dropbox/SZTAKI/Flink/testdata/selfmade/logregdataTEST.csv")
//      .map(x => x.split(","))
//      .map(x => x.toList.map(_.toDouble))
//      .map { x => LabeledVector(x(2), DenseVector(x.take(2).toArray)) }
    //.map(_.vector)

    val trainingDS = env.readTextFile("/home/matefejes/Dropbox/SZTAKI/Flink/testdata/ucla_logreg_tan_norm.csv")
      .map(x => x.split(","))
      .map(x => x.toList.map(_.toDouble))
      .map { x => LabeledVector(x(3), DenseVector(x.take(3).toArray)) } //42

    //trainingDS.writeAsText("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\res0.data")

    val testingDS = env.readTextFile("/home/matefejes/Dropbox/SZTAKI/Flink/testdata/ucla_logreg_tan_norm.csv")
      .map(x => x.split(","))
      .map(x => x.toList.map(_.toDouble))
      .map { x => LabeledVector(x(3), DenseVector(x.take(3).toArray)) }

    //    val trainingDS = env.readTextFile("home/matefejes/Dropbox/SZTAKI/Flink
    // /testdata/otp_ru_2014-15_onehot_szep_mod1_tan.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(40),DenseVector(x.take(40).toArray))} //
    //
    //    //trainingDS.writeAsText("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\res0.data")
    //
    //    val testingDS = env.readTextFile("home/matefejes/Dropbox/SZTAKI/Flink
    // /testdata/otp_ru_2014-15_onehot_szep_mod1_tan.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(40),DenseVector(x.take(40).toArray))}
    //    //.map(_.vector)

    //    val trainingDS = env.readTextFile("C:/Users/User/Dropbox/SZTAKI/Flink
    // /testdata/scale_example_norm.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(1),DenseVector(x.take(1).toArray))} //42
    //
    //    //trainingDS.writeAsText("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\res0.data")
    //
    //    val testingDS = env.readTextFile("C:/Users/User/Dropbox/SZTAKI/Flink
    // /testdata/scale_example_norm.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(1),DenseVector(x.take(1).toArray))}
    //    //.map(_.vector)

    //    val trainingDS = env.readTextFile("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\mlogregr_dim9_norm_tan.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(10),DenseVector(x.take(10).toArray))} //42
    //
    //    //trainingDS.writeAsText("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\res0.data")
    //
    //    val testingDS = env.readTextFile("C:/Users/User/Dropbox/SZTAKI/Flink/
    // testdata/mlogregr_dim9_norm_tan.csv")
    //      .map(x => x.split(";"))
    //      .map(x=>x.toList.map(_.toDouble))
    //      .map{x=>LabeledVector(x(10),DenseVector(x.take(10).toArray))}
    //      //.map(_.vector)


    //
    //       val trainingDS1: DataSet[LabeledVector] = deernumbTrain
    //         .map { (x) => LabeledVector(x._4-1, DenseVector(x._1, x._2, x._3))
    //         }
    //       //trainingDS.print()
    //
    //
    //    val testingDS1: DataSet[Vector] = deernumbTest
    //      .map { x => LabeledVector(x._1, DenseVector(x._1, x._2, x._3)) }
    // .map(_.vector)

    //    testingDS.print()
    //    testingDS1.print()
    //
    //    trainingDS.print()
    //    trainingDS1.print()





      val mlr = org.apache.flink.ml.regression.MultipleLogisticRegression()
        .setIterations(10)
        .setStepsize(1)
        .setConvergenceThreshold(0.0000001)
//        .setMiniBatchRate(0.3)
        .setLearningRateMethod(LearningRateMethod.Default)


      mlr.fit(trainingDS)



      //val predictions = mlr.predict(testingDS)
      val prediction = mlr.predict(testingDS.map(_.vector))
        .map { x => (x._1, x._2, x._2 >= 0.5) }
//
//
//      val thr=0.3


      //
//      val eval = prediction.join(testingDS).where(_._1.toVector)
//        .equalTo(_.vector).map(x => (x._2.label, x._1._2))//,(x._2.label>0)==x._1._3))
////            .map(x=>(x._1._2,x._1._3,x._2.label))

//      eval.writeAsCsv("/home/matefejes/Dropbox/SZTAKI/Flink/testdata/logres/1.csv","\n",",")
//      eval.writeAsText("/home/matefejes/Dropbox/SZTAKI/Flink/testdata/logres/1.csv")
//      env.execute()


//      val evallist=eval.collect()
//      val scalamin=evallist.minBy(_._2)
//      eval.print()
//      val vmi1=eval.reduce{(left,right)=>
//        if (left._2<=right._2)
//          left
//        else
//          right
//      }.collect().head
//
//      val vmimas=eval.min(1).print()//.collect().head._2
//      println(evallist)
//      println(vmi1)
//      println(vmimas)
//      println(scalamin)
//

//      val precision = eval
//
//        .filter(_._2>0.1).map(x=>(1,x._1))
//        .reduce{(left,right)=>(left._1+right._1,left._2+right._2)}
//        .map(x=>x._2/x._1).collect().head

//      val fallout=eval
//        .filter(_._1==0).map(x=>(1.0,round(x._2+(0.5-thr))))
//        .reduce{(left,right)=>(left._1+right._1,left._2+right._2)}
//        .map(x=>x._2/x._1).collect().head
//
//    val recall=eval
//      .filter(_._1==1).map(x=>(x._1,round(x._2+(0.5-thr))))
//      .reduce{(left,right)=>(left._1+right._1,left._2+right._2)}
//      .map(x=>x._2/x._1).collect().head



//    val minPred=eval.min(1).print()//.collect().head._2
//    val maxPred=eval.max(1).print()//.collect().head._2
//    println("minpred"+minPred)
//    println("maxpred"+maxPred)
//    (minPred to maxPred by (maxPred-minPred)/5)
//      print("precision")
//      precision.print()



    //
    //    val recall=eval
    //      .filter(_._1==1).map(x=>(x._1,round(x._2)))
    //      .reduce{(left,right)=>(left._1+right._1,left._2+right._2)}
    //      .map(x=>x._2/x._1)
    //    println("recall")
    //    recall.print()


    //    prediction.print()
    //    prediction1.print()
    //    prediction
    // .writeAsText("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink\\testdata\\res.txt")
    //    prediction1
    // .writeAsText("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink\\testdata\\res1.txt")

    //  eval.writeAsCsv("C:\\Users\\User\\Dropbox\\SZTAKI\\Flink
    // \\testdata\\res10","\n",",")

//        eval.print()
//

    //
    //env.execute
    //    val mlr2 = org.apache.flink.ml.regression.MultipleLogisticRegression()
    //      .setIterations(1)
    //      .setStepsize(1)
    //      .setConvergenceThreshold(0.0000001)
    //      .setMiniBatchRate(0.01)
    //      .setLearningRateMethod(LearningRateMethod.Default)
    //
    //
    //    mlr2.fit(trainingDS)
    //
    //    //val predictions = mlr.predict(testingDS)
    //    val prediction2 = mlr2.predict(testingDS.map(_.vector))
    //      .map { x => (x._1, x._2, x._2 >= 0.5) }
    //
    //    val comp = prediction.join(prediction2).where(_._1.toString).equalTo(_._1.toString)
    //      .map(x=>(x._1._2,x._2._2))
    ////    prediction.print()
    //    comp.print()

  }

}

