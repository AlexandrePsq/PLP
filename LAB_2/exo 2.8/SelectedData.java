package cs.bigdatacourse.lab2;

import java.io.*;

public class SelectedData{

	public static void main(String[] args) throws IOException {
		FileReader fr = null;
		BufferedReader br = null;
		PrintWriter writer = new PrintWriter("result_meteo.txt");
		for (String fileName : args) {
			int lineNo = 0;
			String line;
			fr = new FileReader(fileName);
			br = new BufferedReader(fr);
			try {
				do {
					line = br.readLine();
					lineNo ++ ;
					if ((lineNo > 21) & (line.length() != 0)) {
						writer.println("USAF code: ");
						writer.println(line.substring(0, 0+6));
						writer.println("Name: ");
						writer.println(line.substring(13, 29+13));
						writer.println("Country: ");
						writer.println(line.substring(43, 43+2));
						writer.println("Elevation of the station: ");
						writer.println(line.substring(74, 74+7));
						}
					}
				while(line != null);
			}
			catch(Exception e){
			}
			finally 
			{
				if (fr != null) try {fr.close();} catch (Exception e){};if (br !=null) try {br.close();} catch (Exception e){} 
			}
		}
		writer.close();
	}
}