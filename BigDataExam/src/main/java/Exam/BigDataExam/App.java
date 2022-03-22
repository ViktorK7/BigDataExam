package Exam.BigDataExam;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;


import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;






public class App extends JFrame 
{
	String[] products = {"all products", "Product1", "Product2", "Product3"};
	
	String[] results = {"средна сума", "тотал", "тип плащане тотал", "тип плащане средно"};
	
	
	
	JTextField firstDate = new JTextField();
	JTextField lastDate = new JTextField();
	JTextField countries = new JTextField();
	JTextField cities = new JTextField();
	JCheckBox exSearch = new JCheckBox();
	JComboBox dropProducts= new JComboBox(products);
	JComboBox dropResults = new JComboBox(results);
	
public void init(){
	
	setSize(470,400);
	
	setLayout(null);
	
	JButton button = new JButton("Start Hadoop Project");
	JLabel  date1Text= new JLabel("Начална дата");
    JLabel date2Text = new JLabel("Последна дата");
    JLabel countryText = new JLabel("Държава");
    JLabel cityText = new JLabel("Град");
    JLabel searchText = new JLabel("Точно търсене");
    JLabel productText = new JLabel("Продукти");
    JLabel resultText = new JLabel("Резултат");
	

	
	add(button);
	add(firstDate);
    add(lastDate);
    add(countries);
    add(cities);
    add(exSearch);
    add(dropProducts);
    add(dropResults);
    add(date1Text);
    add(date2Text);
    add(countryText);
    add(cityText);
    add(searchText);
    add(productText);
    add(resultText);
    
	
	
	button.setBounds(100,320,200,30);
	firstDate.setBounds(20,50,130,30);
	lastDate.setBounds(220,50,130,30);
	countries.setBounds(20,120,130,30);
	cities.setBounds(220,120,130,30);
	exSearch.setBounds(170,200,130,30);
	dropProducts.setBounds(20,260,130,30);
	dropResults.setBounds(220,260,130,30);
	date1Text.setBounds(20,20,130,30);
	date2Text.setBounds(220,20,130,30);
	countryText.setBounds(20,90,130,30);
	cityText.setBounds(220,90,130,30);
	searchText.setBounds(140,160,130,30);
	productText.setBounds(20,230,130,30);
	resultText.setBounds(220,230,130,30);

	
	button.addActionListener(new ActionListener() {
		
		@Override
		public void actionPerformed(ActionEvent e) {
			MyMapper.firstDate2 = firstDate.getText();
            MyMapper.lastDate2 = lastDate.getText();
            MyMapper.country = countries.getText();
            MyMapper.city = cities.getText();
            MyMapper.exSearch = exSearch.isSelected();
            MyMapper.product = dropProducts.getSelectedItem().toString();
            MyMapper.result = dropResults.getSelectedItem().toString();
			
			
			runHadoop();
			
		}
	});
	
}

    public static void main( String[] args )
    {
       App app = new App();
       app.init();
       app.setVisible(true);
		}
        
  
        
    
    
    public static void runHadoop() {
		JobClient job = new JobClient(); 
		JobConf conf = new JobConf();
		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/SalesJan2009.csv");
     	Path outputPath = new Path("hdfs://127.0.0.1:9000/output/result");
     	
     	
     	conf.setOutputKeyClass(Text.class);
     	conf.setOutputValueClass(FloatWritable.class);
     	conf.setMapperClass(MyMapper.class);
     	conf.setReducerClass(MyReducer.class);
     	conf.setInputFormat(TextInputFormat.class);
     	conf.setOutputFormat(TextOutputFormat.class);
     	
     	
     	FileInputFormat.setInputPaths(conf, inputPath);
     	FileOutputFormat.setOutputPath(conf, outputPath);
     	
     	
     	try {
     		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
     		
     		if (hdfs.exists(outputPath)) {
     			hdfs.delete(outputPath, true);
     		}

         	job.setConf(conf);
         	job.runJob(conf);
 		} catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
    }
}