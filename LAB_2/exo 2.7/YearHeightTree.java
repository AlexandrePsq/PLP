package cs.bigdatacourse.lab2;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.*;
import java.util.HashMap;


public class YearHeightTree{
	
	public static void main(String[] args) throws FileNotFoundException {
		FileReader fr = null;
		BufferedReader br = null;
		PrintWriter writer = new PrintWriter("result_arbres.txt");
		String year       = new String();
		String height       = new String();
		for (String filename: args) {
			fr = null;
			br = null;
			String line;
			fr = new FileReader(filename);
			br = new BufferedReader(fr);
			try {
				do {
					line = br.readLine();
					HashMap<String, String> tree_data = Tree.get_data(line);
					year   = tree_data.get("ANNEE");
					height = tree_data.get("HAUTEUR");
					writer.println("year: "+year);
					writer.println("height: "+height);
				}
				while(line != null);
			}
			catch(Exception e){
			}
			finally 
			{
				if (fr != null) try {fr.close();} catch (Exception e){};if (br !=null) try {br.close();} catch (Exception e){} }
		}
		writer.close();
		}
}