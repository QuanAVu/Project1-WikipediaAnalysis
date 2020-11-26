package editorCount

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class EditorReducer extends Reducer[Text, IntWritable, Text, IntWritable]{

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    var counter = 0

    values.forEach(counter += _.get())

    context.write(key, new IntWritable(counter/2)) // Divide by two since there are duplicated countries -- bad method
                                                  // Should use a set when mapping

  }

}
