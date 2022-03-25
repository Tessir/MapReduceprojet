/*
  M2 MBDS - Big Data/Hadoop
	Ann��e 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  WCountReduce.java: classe REDUCE.
*/
package org.mbds.hadoop.tp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;


// Notre classe REDUCE - templatee avec un type generique K pour la clef, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class CO2AnalysisReduce extends Reducer<Text, Text, Text, Text>
{
	// La fonction REDUCE elle-meme. Les arguments: la clef key (d'un type generique K), un Iterable de toutes les valeurs
	// qui sont associees a la clef en question, et le contexte Hadoop (un handle qui nous permet de renvoyer le resultat a Hadoop).
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		// Pour parcourir toutes les valeurs associees a la clef fournie.
		int count=0;
		Iterator<Text> i = values.iterator();
		float somme_BonusMalus = 0;
		float sommeRejetC02 = 0;
		float sommeCoutenergie = 0;
		while(i.hasNext()) {
			String[] line = i.next().toString().trim().split(",");
			somme_BonusMalus += Integer.parseInt(line[0]);
			sommeRejetC02 += Float.parseFloat(line[1]);
			sommeCoutenergie += Float.parseFloat(line[2]);
			count++;
		}
		float meanmalus = somme_BonusMalus/count;
		float meanRejet = sommeRejetC02/count;
		float meanCout  = sommeCoutenergie/count;

		context.write(key, new Text( meanmalus +"\t"+ meanRejet + "\t" + meanCout));
  }
}