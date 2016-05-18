package com.damondeng.spark.wiki

import java.io.StringReader

import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.wltea.analyzer.lucene.IKAnalyzer

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
 * Created by DamonDeng on 16/4/6.
 * Designed to be loaded into Spark-Shell to analyze wiki_cn data
 */

class WikiOperator extends java.io.Serializable{


  def isAsciiLetter(c: Char) =  c <= 'z' & !(c>='0' & c<='9')


  /**
   * To convert String into Int
   * @param inputString the input String want to be converted
   * @return the int value of the inputString
   */

  def toInt(inputString: String): Int = {
    try {
      inputString.toInt
    } catch {
      case e: Exception => 0
    }
  }



  def splitStringInto(inputString:String, numberOfChar:Int):Array[String]={

    var result = new ListBuffer[String]()

    val allCharsList = inputString.toCharArray.toList.filter(!isAsciiLetter(_))

    val allChars = allCharsList.toArray

    for(i <- 0 to allChars.length-numberOfChar){

      var tempString = ""
      for (j <- 0 to numberOfChar-1){
        tempString=tempString +allChars(i+j)
      }
      result+=tempString

    }

    result.toArray

  }

  def groupStringInto(inputStrings:Array[String], numberOfString:Int):Array[String]={

    var result = new ListBuffer[String]()


    for(i <- 0 to inputStrings.length-numberOfString){

      var tempString = ""
      for (j <- 0 to numberOfString-1){
        tempString=tempString +inputStrings(i+j)
      }
      result+=tempString

    }

    result.toArray

  }



  /**
   * save WordCount RDD into CSV file.
   * @param counts the RDD to save, the format is (String, Int)
   * @param filePath the path of the file to save
   * @return void
   */
  def saveWordCountToFile(counts:RDD[(String, Int)],filePath:String)={

    val newCounts = counts.map(input => input._1 +","+input._2)

    newCounts.saveAsTextFile(filePath)
  }

  def saveWordCountFloatToFile(counts:RDD[(String, Float)],filePath:String)={

    val newCounts = counts.map(input => input._1 +","+input._2)

    newCounts.saveAsTextFile(filePath)
  }


  /**
   * Load word count RDD from a csv file with format "word,count"
   * @param filePath the path for file to load
   * @return WordCount RDD as RDD[(String, Int)]
   */
  def loadWordCountFromFile(sc:SparkContext, filePath:String):RDD[(String, Int)]={

    val loadCounts = sc.textFile(filePath)

    val counts = loadCounts.map((line:String)=>{val tempResult=line.split(",");(tempResult(0),toInt(tempResult(1)))})

    counts
  }

  def loadTextFileAsWordCount(sc:SparkContext,filePath:String,numberOfChar:Int):RDD[(String,Int)]={
    val inputFile = sc.textFile(filePath)

    val counts = inputFile.flatMap((inputLine:String) => inputLine.split("？|，|。|（|）|、|《|》|：")).flatMap((line:String) => splitStringInto(line,numberOfChar)).map((word:String) => (word, 1)).reduceByKey(_ + _).sortBy(-_._2)

    //  val allWords = inputFile.flatMap((line:String)=> line.toCharArray()).filter(charInput1 => !isAsciiLetter(charInput1)).map((charInput2:Char) => charInput2.toString())

    //  val counts = allWords.map((word:String) => (word, 1)).reduceByKey(_ + _).sortBy(-_._2)

    counts
  }

  def seperateWithIKAnalyzer(inputString:String): Array[String] ={

    val iKAnalyzer = new IKAnalyzer(true)

    val inputStreamReader = new StringReader(inputString)

    val ts:TokenStream = iKAnalyzer.tokenStream("",inputStreamReader)
    val term:CharTermAttribute=ts.getAttribute(classOf[CharTermAttribute])

    var result = new ListBuffer[String]()


    while(ts.incrementToken()){
      //System.out.print(term.toString()+"|")
      result+=term.toString
    }
    iKAnalyzer.close()
    inputStreamReader.close()

    result.toArray


  }

  def analyzeWithIKAnalyzer(inputString:String): String ={

    val iKAnalyzer = new IKAnalyzer(true)

    val inputStreamReader = new StringReader(inputString)

    val ts:TokenStream = iKAnalyzer.tokenStream("",inputStreamReader)
    val term:CharTermAttribute=ts.getAttribute(classOf[CharTermAttribute])

    val result = new StringBuilder()


    while(ts.incrementToken()){
      // System.out.print(term.toString()+"|")
      result.append(term.toString)
      result.append("|")
    }
    iKAnalyzer.close()
    inputStreamReader.close()

    result.toString()


  }

  def loadCountsWithIKAnalyzer(sc:SparkContext,filePath:String):RDD[(String,Int)]={
    val inputFile = sc.textFile(filePath)

    val counts = inputFile.flatMap((inputLine:String) => seperateWithIKAnalyzer(inputLine)).map((word:String) => (word, 1)).reduceByKey(_ + _).sortBy(-_._2)


    counts
  }


  def loadWithIKAnalyzer(sc:SparkContext,filePath:String):RDD[(String)]={
    val inputFile = sc.textFile(filePath)

    val counts = inputFile.map((inputLine:String) => analyzeWithIKAnalyzer(inputLine))


    counts
  }

  def loadWithIKAnalyzerFromLocalSample(sc:SparkContext):RDD[(String)]={

    loadWithIKAnalyzer(sc,"file:///Users/mingxuan/Desktop/workspace/bigdatasource/wiki/AA/wiki_00.txt")
  }



  def loadS3SampleAsWordCount(sc:SparkContext,numberOfChar:Int):RDD[(String,Int)]={

    loadTextFileAsWordCount(sc,"s3n://wiki.bigdata.sig.damondeng.com/simplified/AA/wiki_00.txt",numberOfChar)
  }


  def loadLocalSampleAsWordCount(sc:SparkContext,numberOfChar:Int):RDD[(String,Int)]={

    loadTextFileAsWordCount(sc,"file:///Users/mingxuan/Desktop/workspace/bigdatasource/wiki/AA/wiki_00.txt",numberOfChar)
  }


  def calculateWordPossibility(counts1:RDD[(String,Int)],counts2:RDD[(String, Int)]):RDD[(String,Float)]={

    val counts2_withkey = counts2.flatMap(wordCount => Array((wordCount._1.substring(0,1),wordCount), (wordCount._1.substring(1,2),wordCount)))
    val jointResult = counts2_withkey.join (counts1)
    //jointResult.collect() : Array((鞠,((蹴鞠,1),1)), (鞠,((鞠等,1),1)),

    val singlePossibility = jointResult.map(input => (input._2._1._1 , input._2._1._2.toFloat/input._2._2))
    val doublePossibility = singlePossibility.reduceByKey(_*_)

    val countsAndPossibility = counts2.join(doublePossibility)
    //countsAndPossibility.collect(): Array((种假,(2,6.257528E-5)), (任总,(2,5.4624663E-5)),

    val finalCounts = countsAndPossibility.map(input => (input._1, input._2._2)).sortBy(-_._2)

    finalCounts

  }

  def calculateWordPossibility2(counts1:RDD[(String,Int)],counts2:RDD[(String, Int)]):RDD[(String,Float)]={

    val counts2_withkey = counts2.flatMap(wordCount => Array((wordCount._1.substring(0,1),wordCount), (wordCount._1.substring(1,2),wordCount)))
    val jointResult = counts2_withkey.join (counts1)
    //jointResult.collect();: Array((鞠,((蹴鞠,1),1)), (鞠,((鞠等,1),1)),

    val singlePossibility = jointResult.map(input => (input._2._1._1 , input._2._2/input._2._1._2.toFloat))
    val doublePossibility = singlePossibility.reduceByKey(_-_).map(input => (input._1,Math.abs(input._2)))

    val countsAndPossibility = counts2.join(doublePossibility)
    //countsAndPossibility.collect(): Array((种假,(2,6.257528E-5)), (任总,(2,5.4624663E-5)),

    val finalCounts = countsAndPossibility.map(input => (input._1, input._2._2)).sortBy(-_._2)

    finalCounts

  }


  def calculateWord3Possibility(counts1:RDD[(String,Int)],counts3:RDD[(String, Int)]):RDD[(String,Float)]={

    val counts3_withkey = counts3.flatMap(wordCount => Array((wordCount._1.substring(0,1),wordCount), (wordCount._1.substring(1,2),wordCount),(wordCount._1.substring(2,3),wordCount)))
    val jointResult = counts3_withkey.join (counts1)
    val singlePossibility = jointResult.map(input => (input._2._1._1 , input._2._1._2.toFloat/input._2._2))
    val doublePossibility = singlePossibility.reduceByKey(_*_)

    val countsAndPossibility = counts3.join(doublePossibility)
    //countsAndPossibility.collect(): Array((种假,(2,6.257528E-5)), (任总,(2,5.4624663E-5)),

    val finalCounts = countsAndPossibility.map(input => (input._1, input._2._2)).sortBy(-_._2)

    finalCounts

  }

  def calculateWordPossibilityAndCount(counts1:RDD[(String,Int)],counts2:RDD[(String, Int)]):RDD[(String,Float)]={

    val counts2_withkey = counts2.flatMap(wordCount => Array((wordCount._1.substring(0,1),wordCount), (wordCount._1.substring(1,2),wordCount)))
    val jointResult = counts2_withkey.join (counts1)
    val singlePossibility = jointResult.map(input => (input._2._1._1 , input._2._1._2.toFloat/input._2._2))
    val doublePossibility = singlePossibility.reduceByKey(_*_)

    val countsAndPossibility = counts2.join(doublePossibility)
    //countsAndPossibility.collect(): Array((种假,(2,6.257528E-5)), (任总,(2,5.4624663E-5)),

    val finalCounts = countsAndPossibility.map(input => (input._1, input._2._1 * input._2._2)).sortBy(-_._2)

    finalCounts

  }


  def loadWordPossibilityFromFile(sc:SparkContext,filePath:String):RDD[(String,Float)]={

    val counts1 = loadTextFileAsWordCount(sc,filePath,1)
    val counts2 = loadTextFileAsWordCount(sc,filePath,2)

    calculateWordPossibility(counts1,counts2)
  }



  def loadWordPossibilityFromLocalSampleFile(sc:SparkContext):RDD[(String,Float)]={

    loadWordPossibilityFromFile(sc,"file:///Users/mingxuan/Desktop/workspace/bigdatasource/wiki/AA/wiki_00.txt")

  }


  def calculateWordPossibilityIter(counts1:RDD[(String,Int)],counts2:RDD[(String,Float)],counts3:RDD[(String,Int)]):RDD[(String,Float)]={
    val counts3_split = counts3.flatMap(input=>
      Array((input._1.substring(0,2),input),
        (input._1.substring(1,3),input)))

    val count3_join=counts3_split.join(counts2)
    val count3_join_convert = count3_join.map(input => (input._2._1._1, (input._1 , input._2._2,input._2._1._2)))
    val count3_join_reduce = count3_join_convert.reduceByKey((first, second) => {if (first._2 > second._2){first}else second})
    val back_to_counts2 = count3_join_reduce.map(input => (input._2._1, input._2._3))
    val back_to_counts2_reduce = back_to_counts2.reduceByKey(_+_).sortBy(-_._2)
    val back_to_counts2_possibility = calculateWordPossibility(counts1, back_to_counts2_reduce)

    back_to_counts2_possibility

  }

  def loadWordPossibilityIterFromFile(sc:SparkContext,filePath:String):RDD[(String,Float)]={

    val counts1 = loadTextFileAsWordCount(sc,filePath,1)
    val counts2 = loadTextFileAsWordCount(sc,filePath,2)
    val counts3 = loadTextFileAsWordCount(sc,filePath,3)

    val counts2_possibility=calculateWordPossibility(counts1,counts2)

    calculateWordPossibilityIter(counts1,counts2_possibility,counts3)
  }

  def loadTextFileAsWordCountFromLocalSampleFile(sc:SparkContext,number:Int): RDD[(String, Int)] ={

    loadTextFileAsWordCount(sc,"file:///Users/mingxuan/Desktop/workspace/bigdatasource/wiki/AA/wiki_00.txt",number)
  }


  def loadWordPossibilityIterFromLocalSampleFile(sc:SparkContext):RDD[(String,Float)]={

    loadWordPossibilityIterFromFile(sc,"file:///Users/mingxuan/Desktop/workspace/bigdatasource/wiki/AA/wiki_00.txt")

  }


  def loadWordPossibilityFromS3SampleFile(sc:SparkContext):RDD[(String, Float)]={

    loadWordPossibilityFromFile(sc,"s3n://wiki.bigdata.sig.damondeng.com/simplified/AA/wiki_00.txt")

  }

  def loadWordPossibilityFromS3(sc:SparkContext):RDD[(String,Float)]={
    loadWordPossibilityFromFile(sc,"s3n://wiki.bigdata.sig.damondeng.com/simplified/")


  }


  def calculateWord3PossibilityByMap(counts2_map:Map[String,Float],counts3:RDD[(String, Int)]):RDD[(String,Float)]={

    val finalCounts = counts3.map(wordCount => (wordCount._1, wordCount._2 * counts2_map.get(wordCount._1.substring(0,2)).get * counts2_map.get(wordCount._1.substring(1,3)).get))


    finalCounts

  }






}
