/*
  M2 MBDS - Big Data/Hadoop
	Ann��e 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  WCountMap.java: classe MAP.
*/
package org.mbds.hadoop.tp;

import org.apache.hadoop.io.Text;
import java.util.Objects;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// Notre classe MAP.
public class CO2AnalysisMap extends Mapper<Object, Text, Text, Text> {
	// La fonction MAP elle-m��me.
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String str = value.toString(); //U+20000, represented by 2 chars in java (UTF-16 surrogate pair)
		str = str.replace("\u00a0","");
		String[] line = str.split(",");
		if (line[1].equals("Marque / Modele")) {
			return;
		}
		line[1]=line[1].replaceAll("\"","").trim();
		String[] marquemodel = line[1].split("\\s+");
		String marque = marquemodel[0];
		String malus = line[2];
		malus=malus.replaceAll("\u00a0","").replaceAll("\\s+", "").replace("€1", "").replace("€", "").replace("\"", "");
		if(malus.equals("-")){
			malus="0";
		}
		if (malus.contains("ch"))
		{
			return;
		}
		//recuperation valeur rejet
		String rejetCO2 = line[3].split("€")[0].replaceAll("\\s+", "");
		//recuperation valeur numerique cout
		String coutenergie = line[4].split("€")[0].replaceAll("\\s+","");
		// cout et du rejet
		coutenergie=getDigit(coutenergie);
		rejetCO2= getDigit(rejetCO2);
		int malus_int =Integer.parseInt(malus);
		String new_value = malus_int + "," + rejetCO2 + "," + coutenergie;
		context.write(new Text(marque), new Text(new_value));
		context.write(new Text("MeanFile"), new Text(new_value));
		}

		private String getDigit(String value) {
			if (value == null || value.equals("")) return "";
			StringBuilder caratere = new StringBuilder();
			for (int i = 0; i < value.length(); i++) {
				if (Character.isDigit(value.charAt(i))) {
					caratere.append(value.charAt(i));
				}
			}
			return caratere.toString();
		}
}
