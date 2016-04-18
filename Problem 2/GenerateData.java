package Project3.Query2;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;


public class GenerateData {
    static final String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	public static void main(String[] args) {
		GenerateData gd = new GenerateData();
		try {
			PrintWriter writer = new PrintWriter("CustJson.txt", "UTF-8");
			for(int i = 0; i < 450000; i++){
				writer.println("{Customer ID: " + i +",");
				writer.println("Name: " + generateString(alphabet) + ",");
				writer.println("Address: " + generateString(alphabet) + ",");
				writer.println("Salary: " + gd.generateInt(100, 1000) + ",");
				String s = gd.generateInt(0, 1)>=1 ? "Male" : "Female";
				writer.println("Gender: " + s);
				writer.print("}");
				if (i < 450000) writer.println(",");
			}
			writer.close();
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
    public static String generateString(String characters){
        Random rng = new Random();
        char[] data = new char[100];
        for (int i = 0 ; i < 100; i++){
            data[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(data);
    }

}
