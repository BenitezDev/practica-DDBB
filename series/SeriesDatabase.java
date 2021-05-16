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

		//datos del usuario y servidor
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
		
		//close connection
		try {
			conn.close();
			conn = null;
			System.out.println("Desconectado!");
			return true;

		} catch (Exception e) {
			System.err.println("Error al desconectar " + e.getMessage());
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

		Statement st = null;
		try {

			//ejecuta la accion de crear tabla capitulo
			st = conn.createStatement();
			int result = st.executeUpdate(query);
			System.out.println("Numero de filas afectadas: " + result);
			System.out.println("La query ha sido ejecutada.");
			return true;

		} catch(SQLException se) {
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
		
		PreparedStatement pst = null;
		try {
			//ejecuta la accion de crear tabla valora
			pst = conn.prepareStatement(query);
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
		}catch(Exception e){
			System.out.println("Se produjo un error inesperado");
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

	// TODO: creat catch para "no driver" diapo 25!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

	public int loadCapitulos(String fileName) {

		openConnection();

		int newEntries = 0;
		PreparedStatement ps=null;;
		// SQL
		try{
			conn.setAutoCommit(false);

			// Try to load the .csv
			
			try (BufferedReader bufferLectura = new BufferedReader(new FileReader(fileName));)
			{
				String insert_query = 	"INSERT INTO capitulo (id_serie, n_temporada,n_orden, fecha, titulo, duracion)" + 
						"VALUES (?,?,?,?,?,?);";

				String linea = bufferLectura.readLine();
				linea = bufferLectura.readLine();// no queremos la cabezera del .csv
				

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

				conn.setAutoCommit(true); // hace automaticamente commit
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

		System.out.println("Se han a�adido " + newEntries);
		return newEntries;
	}

	public int loadValoraciones(String fileName) {

		openConnection();

		int newEntries = 0;
		PreparedStatement ps=null;
		// SQL
		try{
			conn.setAutoCommit(true); // por si las moscas. no es necesario
			
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

					// lo mismo interesa pasar los numeros como string y no hacer la basura del valueOf
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

		openConnection();

		String query = "SELECT temporada.id_serie, serie.titulo, temporada.n_temporada, temporada.n_capitulos " +
				"FROM temporada " +
				"INNER JOIN serie ON temporada.id_serie = serie.id_serie " +
				"ORDER BY temporada.id_serie, temporada.n_temporada ;";
		
		Statement st = null;
		ResultSet rs=null;
		try {

			st = conn.createStatement();
			rs = st.executeQuery(query);

			String result = "{";

			int current_serie     = -1;
			int current_temporada = -1;

			

			while (rs.next()) {//mientras que haya filas siga ejecutandose
				
				int id_serie = rs.getInt("id_serie");
				String titulo = rs.getString("titulo");
				int n_temporada = rs.getInt("n_temporada");
				int n_capitulos = rs.getInt("n_capitulos");

				if(current_serie != id_serie){
					if(current_temporada != -1){
						result += "],";
					}else if(result.charAt(result.length() -1) == ']'){
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
		
		PreparedStatement pst1=null;
		ResultSet rs = null;
		try {


			pst1 = conn.prepareStatement(query1);

			pst1.setString(1,  genero);

			rs = 	pst1.executeQuery();
			
			rs.next();

			int existe = rs.getInt("Existe");
			
			System.out.println("La query de comprobacion funciona");
			
			
			
			
			if(existe <= 0){
				throw new Exception("No existe el genero");
				
			} else {
				
			    pst1 = conn.prepareStatement(query2);

				pst1.setString(1,  genero);

			    rs = pst1.executeQuery();
			    
			    rs.next();
				
				float valmedia = rs.getFloat("ValMedia");
				
				int caps = rs.getInt("caps");
				
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
	            pst = conn.prepareStatement(query);
	           
	           
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


