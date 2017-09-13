/* This scripts is used to count the number of occurences of word in a given input file. 
	This script will be run in local mode so giving the local file location of input file.
	To execute this in map reduce mode the input file has to be stored in HDFS 
*/
file = LOAD 'input.txt' USING TextLoader() as (line: chararray); --load input file 
lineTokenize = FOREACH file GENERATE TOKENIZE(line) as words ;-- split into words
flattenWord = FOREACH lineTokenize GENERATE FLATTEN (words) as singleWords; -- bring words out of the bag 
groupWords = GROUP flattenWord by singleWords; -- group by  words ,describe groupWords to check schema groupWords: {group: chararray,flattenWord: {(singleWords: chararray)}}
countWord = FOREACH groupWords GENERATE group , COUNT(flattenWord); -- count the words in the map and store words 
STORE countWord into 'output/' USING PigStorage(','); 
dump countWord; -- display words and count of occurences