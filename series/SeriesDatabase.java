package series;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Blob;
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

		// We open the connection with the data provided in the user and pass
		String serverAddress = "localhost:3306";
		String db = "series";
		String user = "series_user";
		String pass = "series_pass";
		String url = "jdbc:mysql://" + serverAddress + "/" + db;


		try{

			// We check if a connection has been opened before
			if(conn != null){
				System.out.println("Anteriormente conectado");
				return false;
			}
			// 	We open the connection in case of not having been open previously
			conn = DriverManager.getConnection(url, user, pass);
			System.out.println("Conectado a la base de datos!");

			return true;
		
		} catch(Exception e){
			System.err.println("Error al conectar la BD" + e.getMessage());
			e.printStackTrace();
			return false;
		}
		
		
	}

	public boolean closeConnection() {
		
		// We close the connection and take it to the null state to be able to work with it and check if it is closed.
		try {
			conn.close();
			conn = null;
			System.out.println("Desconectado!");
			return true;

		} catch (Exception e) {
			System.err.println("Error al desconectar " + e.getMessage());
			e.printStackTrace();
			return false;
		}



	}

	public boolean createTableCapitulo() {

		// We open connection in case it has not been opened previously
		openConnection();


		// We define in SQL language the query that we are going to perform in the database
		// As it does not have parameters, we use the Statement class
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

		Statement st = null;
		try {

			st = conn.createStatement();
			
			// We dump the result in an integer to check if it agrees with our interests. It must return 0 because not
			// we modify anything existing
			int result = st.executeUpdate(query);
			System.out.println("Numero de filas afectadas: " + result);
			System.out.println("La query ha sido ejecutada.");
			return true;

		} catch(SQLException se) {
			// In case of error we call to the SQL compiler so we can Know if there is anything wrong with the Workbench
			System.out.println("Mensaje de error: " + se.getMessage() );
			System.out.println("Código de error: " + se.getErrorCode() );
			System.out.println("Estado SQL: " + se.getSQLState() );
			se.printStackTrace();
			return false;
		}catch(Exception e){
			System.out.println("Se produjo un error inesperado");
			return false;
		}finally{
			try{
				System.out.println("Ejecuto finally \n");
				if(st!=null) st.close();
			}catch(SQLException e){
				e.printStackTrace();

			}
		}

	}
	
	public boolean createTableValora() {
		
		
		// We open connection in case it has not been opened previously
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
		Statement st = null;
		try {
			// As in the previous method we use Statement class because we do not have parameters in the query
			st = conn.createStatement();
			int result = st.executeUpdate(query);
			
			System.out.println("Numero de filas afectadas: " + result);
			System.out.println("La query ha sido ejecutada.");
			return true;

		} catch(SQLException se) {
			// In case of error we call to the SQL compiler so we can Know if there is anything wrong with the Workbench
			System.out.println("Mensaje de error: " + se.getMessage() );
			System.out.println("Codigo de error: " + se.getErrorCode() );
			System.out.println("Estado SQL: " + se.getSQLState() );
			se.printStackTrace();
			return false;
		}catch(Exception e){
			System.out.println("Se produjo un error inesperado");
			return false;
		}finally{
			try{
				System.out.println("Ejecuto finally \n");
				if(st!=null) st.close();
			}catch(SQLException e){
				e.printStackTrace();

			}
		}
	}

	

	public int loadCapitulos(String fileName) {

		openConnection();
		
		// Counter to see how many items we have added to the database
		int newEntries = 0;
		PreparedStatement ps=null;;
		// SQL
		try{
			// We need to do all the insert in one transaction so we set Autocommit to false in order to avoid that the commit occurs in each step.
			conn.setAutoCommit(false);

			// Try to load the .csv
			
			try (BufferedReader bufferLectura = new BufferedReader(new FileReader(fileName));)
			{
				
				String insert_query = 	"INSERT INTO capitulo (id_serie, n_temporada,n_orden, fecha, titulo, duracion)" + 
						"VALUES (?,?,?,?,?,?);";

				String linea = bufferLectura.readLine();
				// WE do not want to read the very first line of the document because it contains useless text.
				linea = bufferLectura.readLine(); 
				// In this case we have to use PreparedStatement because we have some parameters in the query that we want to fill in a loop
				// to make it more efficient

				
				while (linea != null) 
				{
					String[] campo = linea.split(";"); 

					// [0] = id_serie  [1] = n_temporada 
					// [2] = n_orden   [3] = fecha         
					// [4] = titulo    [5] = duracion  
					ps = conn.prepareStatement(insert_query);
					ps.setString(1, campo[0]);
					ps.setString(2, campo[1]);
					ps.setString(3, campo[2]);
					ps.setString(4, campo[3]);
					ps.setString(5, campo[4]);
					ps.setString(6, campo[5]);

					newEntries++;

					ps.executeUpdate();

					linea = bufferLectura.readLine();					
				}

				// This put the SQL Commit structure in automatic as we have earlier and also make a commit in case of success.
				conn.setAutoCommit(true); 
			} 
			catch (IOException e) 
			{
				// To check an error in the read phase
				System.err.println("No se puede leer el archivo.");
				e.printStackTrace();
			}
		}
		catch(SQLException se)
		{
			se.printStackTrace();
			System.out.println("Mensaje de error: " + se.getMessage() );
			System.out.println("Codigo de error: " + se.getErrorCode() );
			System.out.println("Estado SQL: " + se.getSQLState() );
			newEntries = 0;
		}catch(Exception e){
			System.out.println("Se produjo un error inesperado");
		}finally{
			try{
				System.out.println("Ejecuto finally \n");
				if(ps!=null) ps.close();
			}catch(SQLException e){
				e.printStackTrace();

			}
		}

		System.out.println("Se han a�adido " + newEntries);
		return newEntries;
	}

	public int loadValoraciones(String fileName) {

		openConnection();

		int newEntries = 0;
		PreparedStatement ps=null;
		// SQL
		try{
			// Now we need to make a commit in each insert so we want to be sure that Autocommit is activated
			conn.setAutoCommit(true);

			// Try to load the .csv
			try (BufferedReader bufferLectura = new BufferedReader(new FileReader(fileName));)
			{
				String insert_query = 	"INSERT INTO valora (id_serie, n_temporada, n_orden, id_usuario, fecha, valor)" + 
						"VALUES (?,?,?,?,?,?);";

				String linea = bufferLectura.readLine();
				linea = bufferLectura.readLine();// no queremos la cabezera del .csv

				 

				//			


				int x = 0;
				while (linea != null) {
					String[] campo = linea.split(";"); 
					ps = conn.prepareStatement(insert_query);


					ps.setInt	(1, Integer.valueOf(campo[0]));
					ps.setInt	(2, Integer.valueOf(campo[1]));
					ps.setInt	(3, Integer.valueOf(campo[2]));
					ps.setInt	(4, Integer.valueOf(campo[3]));
					ps.setString(5, campo[4]);
					ps.setInt	(6, Integer.valueOf(campo[5]));

					newEntries++;

					ps.executeUpdate();

					linea = bufferLectura.readLine();		
					//					x++;
				}

			} 
			catch (IOException e) 
			{
				System.out.println("No se puede leer el archivoooooooooooooo.");
				e.printStackTrace();
			}
		}
		catch(SQLException se)
		{
			se.printStackTrace();
			System.out.println("Mensaje de error: " + se.getMessage() );
			System.out.println("Código de error: " + se.getErrorCode() );
			System.out.println("Estado SQL: " + se.getSQLState() );

			newEntries = 0;
		}catch(Exception e){
			System.out.println("Se produjo un error inesperado");
		}finally{
			try{
				System.out.println("Ejecuto finally \n");
				if(ps!=null) ps.close();
			}catch(SQLException e){
				e.printStackTrace();

			}
		}

		System.out.println("Se han anyadido " + newEntries);
		return newEntries;
	}

	public String catalogo() {
		// We open connection in case it has not been opened previously
		openConnection();


		String query = "SELECT temporada.id_serie, serie.titulo, temporada.n_temporada, temporada.n_capitulos " +
				"FROM temporada " +
				"INNER JOIN serie ON temporada.id_serie = serie.id_serie " +
				"ORDER BY temporada.id_serie, temporada.n_temporada ;";
		
		Statement st = null;
		ResultSet rs=null;
		try {

			// We define in SQL language the query that we are going to perform in the database
			// As it does not have parameters, we use the Statement class
			//We have to use the ResultSet class in order to display the result on the terminal
			st = conn.createStatement();
			rs = st.executeQuery(query);

			String result = "{";

			
			// These two parameters will help to establish a difference between two different series as we check serie_id and n_temporada
			int current_serie     = -1;
			int current_temporada = -1;

			
			// Until we reach the end of the table we need to execute the while
			while (rs.next()) {
				
				int id_serie = rs.getInt("id_serie");
				String titulo = rs.getString("titulo");
				int n_temporada = rs.getInt("n_temporada");
				int n_capitulos = rs.getInt("n_capitulos");

				
				
				// All these if's basically study all the possible cases (no series in the database or no chapters in a series)
				// On each case we have to write something different
				if(current_serie != id_serie){
					if(current_temporada != -1){
						result += "],";
					
					} // With charAt we can check what is the last character of the string in order to complete it correctly
					else if(result.charAt(result.length() -1) == ']'){
						result += ",";
					}

					current_serie = id_serie;
					current_temporada = -1;
					result += titulo;
					result += ":";
				}

				if(current_temporada == -1){
					result += "[";
				}else{
					result += ",";	
				}

				current_temporada = n_temporada;

				if(n_temporada != 0)
				{
					result += n_capitulos;	
				}
			}
			
			if (result.charAt(result.length() -1) == ']'){
				result += "}";
			}else if (result.charAt(result.length() -1) == '{'){
				result += "}";
			}else{
				result += "]}";	
			}
			
			// The result string has spaces instead of _ so we need to change them
			return result.replaceAll(" ", "_");
			
		} catch(SQLException se) {
			System.out.println("Mensaje de error: " + se.getMessage() );
			System.out.println("Código de error: " + se.getErrorCode() );
			System.out.println("Estado SQL: " + se.getSQLState() );
			se.printStackTrace();
			return null;
		}catch(Exception e){
			System.out.println("Se produjo un error inesperado");
			return null;
		}finally{
			try{
				System.out.println("Ejecuto finally \n");
				if(st!=null) st.close();
				if(rs!=null) rs.close();
			}catch(SQLException e){
				e.printStackTrace();

			}
		}

	}

	public double mediaGenero(String genero) {
		
		openConnection();
		
		String query1 =  "SELECT COUNT(DISTINCT genero.descripcion) Existe " +
						 "FROM genero " +
						 "WHERE genero.descripcion = ? ;";
		
		String query2 =  "SELECT COUNT(valora.id_serie) caps, AVG(valor) ValMedia " +
				 		 "FROM valora " +
				 		 "INNER JOIN serie ON serie.id_serie = valora.id_serie " +
				 		 "INNER JOIN pertenece ON pertenece.id_serie = valora.id_serie " +
				 		 "INNER JOIN genero ON pertenece.id_genero = genero.id_genero " +
				 		 "WHERE genero.descripcion = ? ;";
		
		PreparedStatement pst1 = null;
		ResultSet rs = null;
		try {

			// First query is going to check if the film genre that the user will pass exits in the DataBase using a counter
			// We have to use PreparedStatement because we have parameters in both queries. 
			pst1 = conn.prepareStatement(query1);

			pst1.setString(1,  genero);

			rs = pst1.executeQuery();
			
			// We know that rs is pointing to and empty place just above the first element of the table so we have to use the next() method to go one row below
			rs.next();

			int existe = rs.getInt("Existe");
			
			System.out.println("La query de comprobacion funciona");
			
			pst1.close();
			rs.close();
			
		
			if(existe <= 0){
				throw new Exception("No existe el genero");
				
			} else {
				
				// In case of existence  we execute the second query 
			    pst1 = conn.prepareStatement(query2);

				pst1.setString(1,  genero);

			    rs = pst1.executeQuery();
			    
			    rs.next();
				
				float valmedia = rs.getFloat("ValMedia");
				
				int caps = rs.getInt("caps");
				
				// If a series have 0 chapters we return a ValMedia = 0 and if not we return the exact value.
				if(caps <= 0)
					{return 0.0;}
				
				return valmedia;
				
			}



		} catch (SQLException se) {
			System.out.println("Mensaje de error: " + se.getMessage() );
			System.out.println("Código de error: " + se.getErrorCode() );
			System.out.println("Estado SQL: " + se.getSQLState() );
			se.printStackTrace();
			return -2;
			
		} catch (Exception e) {
			return -1;

		}finally{
			try{
				System.out.println("Ejecuto finally \n");
				if(pst1!=null) pst1.close();
				if(rs!=null) rs.close();
			}catch(SQLException e){
				e.printStackTrace();

			}
		}
		
	}

	public boolean setFoto(String filename) {
		openConnection();
		
	        String query = "UPDATE usuario " + 
	        			"SET fotografia = ? " + 
	        			"WHERE apellido1 = 'Cabeza';";
	        
	        PreparedStatement pst=null;
	        try {
	        	// For security we want to take the image as a parameter so we have to use PreparedStatement
	            pst = conn.prepareStatement(query);
	           
	            // We read the file and we introduce it in the query to execute the query
	            File file = new File("src/" + filename);
	            FileInputStream fis = new FileInputStream(file);
	            pst.setBinaryStream(1, fis, (int)file.length());
	            pst.executeUpdate();
	            
	            
	            //PARA VER SI SE GUARDA CORRECTAMENTE LA FOTOGRAFIA EN LA BASE DE DATOS LA EXTRAIGO DE AHI
//	            Statement st  = conn.createStatement();
//	            ResultSet rs = st.executeQuery("select fotografia  from usuario where apellido1 = 'Cabeza';");
//	            
//	            byte data[] = null;
//	            Blob myBlob = null;
//	            
//	            while(rs.next()){
//	            	myBlob = rs.getBlob("fotografia");
//	            	data = myBlob.getBytes(1, (int)myBlob.length());
//	            }
//	            
//	            FileOutputStream fos = new FileOutputStream("C:/Users/mvW10/Downloads/descarga.jpg");
//	            fos.write(data);
//	            fos.close();
//	            System.out.println("fichero guardado");
	            return true;
	           
	        } catch (SQLException se) {
	        	System.out.println("Mensaje de error: " + se.getMessage() );
				System.out.println("Código de error: " + se.getErrorCode() );
				System.out.println("Estado SQL: " + se.getSQLState() );
	            se.printStackTrace();
	            return false;
	        } catch (FileNotFoundException e) {
	           
	            System.out.println("No se encuentra el archivo");
	            e.printStackTrace();
	            return false;
	        } catch (IOException e) {
			
				e.printStackTrace();
				return false;
			}finally{
				try{
					System.out.println("Ejecuto finally \n");
					if(pst!=null) pst.close();
				}catch(SQLException e){
					e.printStackTrace();

				}
			}
	       
	    }
		
}


