package editorCount

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class EditorMapper extends Mapper[LongWritable, Text, Text, IntWritable]{

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    // Convert each line from byte to readable strings
    val line = value.toString

    // Split the line content by tabs
    // Look for countries with editors who edited the enwiki
    line.split("\\t").filter(_.length > 0).foreach((word: String) => {
      if(word == "enwiki"){
        context.write(new Text(word), new IntWritable(1))
      }
    })

  }

}
