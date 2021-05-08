package series;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


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
		
		
		openConnection();
		
		String query0 = "DROP TABLE IF EXISTS capitulo;";
		String query =  
						
						"CREATE TABLE capitulo ( " +
						"id_serie INT," +
						"n_temporada INT," +
						"n_orden INT," +
						"duracion INT," +
						"titulo VARCHAR(100)," +
						"fecha DATE," +
						"PRIMARY KEY (id_serie, n_temporada, n_orden)," +
						"FOREIGN KEY (id_serie, n_temporada) REFERENCES temporada (id_serie, n_temporada)" +
						"ON DELETE CASCADE ON UPDATE CASCADE);";
		
		
		
		
		try {
			PreparedStatement ps1 = conn.prepareStatement(query0);
//			Statement st = conn.createStatement();
			PreparedStatement pst = conn.prepareStatement(query);
			
			
			int result0 = ps1.executeUpdate(query0);
			int result = pst.executeUpdate(query);
			System.out.println("Numero de filas afectadas: " + result);
			System.out.println("La query ha sido ejecutada.");
			return true;
			
			} catch(SQLException se) {
				System.out.println("Mensaje de error: " + se.getMessage() );
				System.out.println("Código de error: " + se.getErrorCode() );
				System.out.println("Estado SQL: " + se.getSQLState() );
				se.printStackTrace();
				return false;
		}
		
		
		
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
