package series;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class SeriesDatabase {
	
	private Connection conn;

	public SeriesDatabase() {

	}

	public boolean openConnection() {
		

		//String drv = "com.mysql.jdbc.Driver";
        //Class.forName(drv);
		
	
        String serverAddress = "localhost:3306";
        String db = "series";
        String user = "series_user";
        String pass = "series_pass";
        String url = "jdbc:mysql://" + serverAddress + "/" + db;
        
       
		try{

			// check if there is a connection
			 if(conn != null){
				 System.out.println("Anteriormente conectado");
		         return false;
		     }
			// create connection
	        conn = DriverManager.getConnection(url, user, pass);
	        System.out.println("Conectado a la base de datos!");
	        
	        return true;
		}
		catch(Exception e){
			System.err.println("Error al conectar la BD" + e.getMessage());
	        e.printStackTrace();
			return false;
		}
		
	}

	public boolean closeConnection() {
		
		
		try {
			conn.close();
			conn = null;
			System.out.println("Desconectado!");
			return true;
			
		} catch (Exception e) {
			System.err.println("Error al desconectar " + e.getMessage());
//	        e.printStackTrace();
			return false;
		}
	
		
		
	}

	public boolean createTableCapitulo() {
		return false;
	}

	public boolean createTableValora() {
		return false;
	}

	public int loadCapitulos(String fileName) {
		return 0;
	}

	public int loadValoraciones(String fileName) {
		return 0;
	}

	public String catalogo() {
		return null;
	}

	public double mediaGenero(String genero) {
		return 0.0;
	}

	public boolean setFoto(String filename) {
		return false;
	}

}
