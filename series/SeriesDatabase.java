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
		
			PreparedStatement pst = conn.prepareStatement(query);
			
			
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
	openConnection();
		
		String query =  
						
						"CREATE TABLE valora ( " +
						"fecha DATE," +
						"id_serie INT," +
						"n_temporada INT," +
						"n_orden INT," +
						"id_usuario INT," +
						"valor INT," +
						"PRIMARY KEY (fecha, id_serie, n_temporada, n_orden, id_usuario)," +
						"FOREIGN KEY (id_serie, n_temporada, n_orden) REFERENCES capitulo (id_serie, n_temporada, n_orden)," +
						"FOREIGN KEY (id_usuario) REFERENCES usuario (id_usuario)" +
						"ON DELETE CASCADE ON UPDATE CASCADE);";
	
		try {
		
			PreparedStatement pst = conn.prepareStatement(query);
			
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

	public int loadCapitulos(String fileName) {
		
		conn.setAutoCommit(false);
		
		// INSERT DESDE PREPARED ST CICLO 
		// exce
		if(result = 0) //fallo porqueno inserta nada
			conn.rollback();
		else conn.commit();
		
		conn.setAutoCommit(true); //commit.
		
		return 0;
//		
//		private void insercionPrepared() throws Exception {
//			String nombre[] = {"Clara", "Dani", "Edward", "Denzel"};
//			String apellido[] = {"Lago", "Rovira", "Norton", "Washington"};
//			
//			// Sin ID porque es autoincremental
//			
//			String query = "INSERT INTO actor (first_name, last_name, last_update) VALUES (?,?,?);";
//			
//			PreparedStatement pst = conn.prepareStatement(query);
//			
//			for (int i = 0; i < nombre.length; i++) {
//				pst.setString(1,  nombre[i]);
//				pst.setString(2, apellido[i]);
//				//pst.setDate(3, new java.sql.Date(new java.util.Date().getTime()));
//				pst.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()));
//				int res = pst.executeUpdate();
//				System.out.println("Insertado correctamente " + ((res == 1)?"Si":"No"));
//			}
//			
//			System.out.println("query ejecutada");
//			pst.close();
//			conn.close();  		
//		}
		
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
