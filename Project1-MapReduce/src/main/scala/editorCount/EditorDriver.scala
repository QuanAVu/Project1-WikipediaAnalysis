package editorCount

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job

object EditorDriver extends App{

  if(args.length != 2){
    println("Usage: EditorDriver <input dir> <output dir>")
    System.exit(-1)
  }

  // instantiating a Job object we can configure
  val job = Job.getInstance()

  // Set the jar file that contains driver, mapper, and reducer.
  // This jar file will be transferred to nodes that run tasks
  job.setJarByClass(EditorDriver.getClass)

  // Specify a job name, for us and other devs
  job.setJobName("Enwiki Country Count")

  // Specify <input> and <output> paths based on command line args
  FileInputFormat.setInputPaths(job, new Path(args(0)))

  //This line sets output based on the second
  FileOutputFormat.setOutputPath(job, new Path(args(1)))

  // Specify Mapper and Reducer
  job.setMapperClass(classOf[EditorMapper])
  job.setReducerClass(classOf[EditorReducer])

  // Specify the job's output key and value classes. We're making use of some
  // default to not have to specify input and intermediate
  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[IntWritable])

  val success = job.waitForCompletion(true)
  System.exit(if (success) 0 else 1)

}
