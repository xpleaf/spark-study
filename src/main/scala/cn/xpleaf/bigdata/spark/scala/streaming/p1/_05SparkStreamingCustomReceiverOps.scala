package cn.xpleaf.bigdata.spark.scala.streaming.p1

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming自定义Receiver
  * 通过模拟Network来学习自定义Receiver
  *
  * 自定义的步骤：
  *     1.创建一个类继承一个类或者实现某个接口
  *     2.复写启动的个别方法
  *     3.进行注册调用
  */
object _05SparkStreamingCustomReceiverOps {
    def main(args: Array[String]): Unit = {
        if (args == null || args.length < 2) {
            System.err.println(
                """Parameter Errors! Usage: <hostname> <port>
                  |hostname: 监听的网络socket的主机名或ip地址
                  |port：    监听的网络socket的端口
                """.stripMargin)
            System.exit(-1)
        }
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setAppName(_05SparkStreamingCustomReceiverOps.getClass.getSimpleName)
            .setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(5))
        val hostname = args(0).trim
        val port = args(1).trim.toInt

        val linesDStream:ReceiverInputDStream[String] = ssc.receiverStream[String](new MyNetWorkReceiver(hostname, port))
        val retDStream:DStream[(String, Int)] = linesDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

        retDStream.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}

/**
  * 自定义receiver
  */
class MyNetWorkReceiver(storageLevel:StorageLevel) extends Receiver[String](storageLevel) {

    private var hostname:String = _
    private var port:Int = _

    def this(hostname:String, port:Int, storageLevel:StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2) {
        this(storageLevel)
        this.hostname = hostname
        this.port = port
    }

    /**
      * 启动及其初始化receiver资源
      */
    override def onStart(): Unit = {
        val thread = new Thread() {
            override def run(): Unit = {
                receive()
            }
        }
        thread.setDaemon(true)  // 设置成为后台线程
        thread.start()
    }

    // 接收数据的核心api 读取网络socket中的数据
    def receive(): Unit = {
        val socket = new Socket(hostname, port)
        val ins = socket.getInputStream()
        val br = new BufferedReader(new InputStreamReader(ins))
        var line:String = null
        while((line = br.readLine()) != null) {
            store(line)
        }
        ins.close()
        socket.close()
    }

    override def onStop(): Unit = {

    }
}