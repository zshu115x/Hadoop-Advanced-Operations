package Project3.Query1;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class GenerateData {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		GenerateData gd = new GenerateData();
		try {
			PrintWriter writer = new PrintWriter("P.txt", "UTF-8");
			for(int i = 0; i < 11000000; i++){
				int x = gd.generateInt(1, 10000);
				int y = gd.generateInt(1, 10000);
				String point = "" + x + "," + y;
				if (i < 11000000) writer.println(point);
			}
			writer.close();
			
			PrintWriter writer2 = new PrintWriter("R.txt", "UTF-8");
			for(int i = 0; i < 8000000; i++){
				int top_x = gd.generateInt(1, 10000);
				int top_y = gd.generateInt(1, 10000);
				int height = gd.generateInt(1, 20);
				int width = gd.generateInt(1, 5);
				String rec = "R" + i +"," + top_x + "," + top_y + ","
						+ height + "," + width;
				if (i < 8000000) writer2.println(rec);
			}
			writer2.close();
			System.out.println("Success!");
			
		}catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	private Random rand = new Random();
	
	public int generateInt(int min, int max){
		int randNumber = rand.nextInt(max-min+1) + min;
		return randNumber;
	}

}
