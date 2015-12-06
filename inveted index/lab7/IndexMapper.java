package stubs;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

	private Text orginalWord = new Text();
	private Text pathNumber = new Text();

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
	InterruptedException {

		String[] line = value.toString().split("\\W+");
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		Path path = fileSplit.getPath();
		String fileName = path.getName();
		fileName.replaceAll(" ", "");

		if(line.length > 0)
		{
			for(String word : line)
			{
				if(word.length() > 0)
				{
					orginalWord.set(word);
					pathNumber.set(fileName+"@"+key.toString()); // conc the path to the number of line 

					context.write(orginalWord, pathNumber);
				}
			}
		}
	}
}