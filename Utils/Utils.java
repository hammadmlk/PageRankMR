package Utils;

public class Utils {
	
	
	public static double str2double(String str){
		return Double.parseDouble(str);
	}
	
	public static String double2str(double d){
		return Double.toString(d);
	}
	
	public static int str2int(String str){
		return Integer.parseInt(str);
	}
	
	public static String int2str(int i){
		return Integer.toString(i);
	}
	
	//TODO: should write to a readable log file.
	public static <T> void log(T input){
		System.out.println(input);
	}
	
}
