# Project1-WikipediaAnalysis

## Project Description
Project 1's analysis consists of using big data tools to answer questions about datasets from Wikipedia. There are a series of basic analysis questions, answered using Hive or MapReduce. The tool(s) used are determined based on the context for each question. The output of the analysis includes MapReduce jarfiles and/or .hql files so that the analysis is a repeatable process that works on a larger dataset, not just an ad hoc calculation. Assumptions and simplfications are required in order to answer these questions, and the final presentation of results includes a discussion of those assumptions/simplifications and the reasoning behind them. In addition to answers and explanations, this project requires a discussion of any intermediate datasets and the reproduceable process used to construct those datasets. Finally, in addition to code outputs, this project requires a simple slide deck providing an overview of results. The questions follow: 
1. Which English wikipedia article got the most traffic on October 20, 2020? 
2. What English wikipedia article has the largest fraction of its readers follow an internal link to another wikipedia article? 
3. What series of wikipedia articles, starting with Hotel California, keeps the largest fraction of its readers clicking on internal links? 
4. Find an example of an English wikipedia article that is relatively more popular in the UK, then find the same for the US and Australia. 
5. How many users will see the average vandalized wikipedia page before the offending edit is reversed? 
6. Run an analysis to find how many countries that have editors who edited an English Wikipedia page.

## Technologies Used
- Scala - version 2.13.3
- SBT - version 1.4.3
- Hadoop - version 3.2.1
- HDFS
- YARN
- Hive - version 3.1.2
- MapReduce - version 3.2.1

## Features
List of features ready and TODOs for future development

- Read large tsv files and store inside Hive tables up in HDFS.
- Finding the most popular English wikipedia articles. (Could also find articles that are not English)
- Finding the English wikipedia article that has the most clicked internal links.
- Finding series of related articles using nested queries.
- Using MapReduce jar to take in an input file of Wikipedia and perform data calculation.

To-do list:

- Try to simplify queries using more Join conditions and less nested queries.
- Cut code lines in Hive QL file to be shorter and cleaner.

## Getting Started
To setup you will need GitBash, Ubuntu (for Hadoop & Hive), and IntelliJ to generate a MapReduce jar.
You will also need a sql text editor which helps in fast sql queries delivery inside Ubuntu.
1. Start your GitBash and locate to the directory you want to have the repo cloned. Then use the command: ***git clone https://github.com/QuanAVu/Project1-WikipediaAnalysis.git***.
2. Now open the file *Project1-MapReduce* inside IntelliJ as a project and then generate a new jar using the command ***package*** in sbt console. This should give you a new "thin" jar file under the target->scala-2.12 directory.
3. Open Ubuntu and make sure you have hadoop-3.2.1 unzipped file (unzipping in Ubuntu: ***gzip -d filename***). Now to connect to HDFS and YARN use the commands: ***sudo service ssh start*** then ***hadoop-3.2.1/sbin/start-hdfs.sh*** and ***hadoop-3.2.1/sbin/start-yarn.sh***. This will let us connect to our distributed system and we can start using our MapReduce jar file.
4. Use command ***cd hadoop-3.2.1/*** to go to the hadoop directory. Make sure you create a folder called *input* and store your wikipedia file (https://dumps.wikimedia.org/other/geoeditors) inside that input file. 
5. Also connect your Ubuntu home directory to your GitBash directory where you stored your jar file. To do so, inside your Ubuntu use command: ***ln -s /mnt/c/Users/me/gitbashdirectory/***. Then just ***cd ..*** to go back to home directory in Ubuntu.
6. Download Hive 3.1.2 from the Apache website. Also unzipped it in your home directory. To run Hive use command: ***beeline -u jdbc:hive2://***.

## Usage
To answer analyses 1-5 all you need is Hive QL and SQL in your Ubuntu. Inside your Ubuntu run the beeline command above and make sure that you're connected to HDFS and YARN. Then follow the order of codes inside the *Hive_SQL.sql* file cloned in your repo. Copy each of the code chunks and paste each of them inside Hive one at a time and run each of them. They will give you back the answers in table form. **Note** that you will need to load data into Hive tables (https://dumps.wikimedia.org/other/analytics/) before you run the queries.

To answer last question you will need to run the jar file inside *hadoop-3.2.1* directory. Using the command ***bin/hadoop jar ../IdeaProjects/editorCount/target/scala-2.13/editorCount.jar input output***. This will perform the MapReduce jar on the wikipedia data you stored inside the input file in Ubuntu.

## License
Copyright (c) 2020 Quan A Vu

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
