package sample_update;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;


public class database_v1 {

	Connection con;
	String url ;
	String password ;
	String user ;
	static Statement stmt;
	Statement stmt1;
	PreparedStatement prepforall = null;

	public database_v1() throws ParserConfigurationException, SAXException, IOException {
		
		Setting.Loadsetting();
		try {
			Class.forName("org.postgresql.Driver").newInstance();
			url = Setting.databaseUrl;
			user = Setting.databaseUserName;
			password = Setting.databasePassword;
			con = DriverManager.getConnection(url, user, password);
			con.setAutoCommit(true);
			stmt = con.createStatement();
			stmt1 = con.createStatement();



		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public int getid(String table) throws SQLException {
		int newid = 0;
		String query = "SELECT max(id) from " + table;
		System.out.println("query " + query);
		ResultSet resultSet = stmt.executeQuery(query);
		while (resultSet.next()) {
			query = resultSet.getString("max");
		}
		if (query == null)
			newid = 1;
		else
			newid = Integer.valueOf(query) + 1;

		System.out.println("id number in db " + newid);
		return newid;

	}

	public int getspecies_id(int tax_id) throws SQLException {
		String query = "select id from species where tax_id=" + tax_id + ";";
		// System.out.println("query"+query);
		ResultSet resultSet = stmt.executeQuery(query);

		int id = 0;
		while (resultSet.next()) {
			id = resultSet.getInt("id");

		}

		return id;

	}

	public int getattribute_id(String name) throws SQLException {
		String query = "select id from attribute where lower(attribute_name)=" + "lower('" + name + "');";
		// System.out.println("query"+query);
		ResultSet resultSet = stmt.executeQuery(query);

		int id = 0;
		while (resultSet.next()) {
			id = resultSet.getInt("id");

		}
		if (id == 0) {

			String query1 = "select id from attribute where lower(structured_comment_name)=" + "lower('" + name + "');";
			// System.out.println("query"+query);
			ResultSet resultSet1 = stmt.executeQuery(query1);

			while (resultSet1.next()) {
				id = resultSet1.getInt("id");

			}
		}

		return id;

	}
	
	public int getdataset_id(String doi) throws SQLException
	{
		String query="select id from dataset where identifier="+"'"+doi+"'"+";";
		//System.out.println("query"+query);
		ResultSet resultSet=stmt.executeQuery(query);
		
		int id=0;
		while(resultSet.next())
		{
			id= resultSet.getInt("id");
	
		}
		
		return id;

	}

	public String getunit_id(String name) throws SQLException {
		String query = "select id from unit where name=" + "'" + name + "';";
		// System.out.println("query"+query);
		ResultSet resultSet = stmt.executeQuery(query);

		String id = null;
		while (resultSet.next()) {
			id = resultSet.getString("id");

		}

		return id;

	}

	public int add_attribute(String name) throws SQLException {
		int id = this.getid("attribute");
		String query1 = "insert into attribute(id,attribute_name) values(?,?)";
		PreparedStatement prep1 = null;
		prep1 = con.prepareStatement(query1);
		prep1.setInt(1, id);
		prep1.setString(2, name);
		prep1.executeUpdate();

		return id;
	}

	public int addspeciestable(int taxid, String name) throws SQLException {
		int id = this.getid("species");
		String query1 = "insert into species(id,tax_id,scientific_name) values(?,?,?)";
		PreparedStatement prep1 = null;
		prep1 = con.prepareStatement(query1);
		prep1.setInt(1, id);
		prep1.setInt(2, taxid);
		prep1.setString(3, name);
		prep1.executeUpdate();
		return id;
	}

	public void addsamplev3(int datasetid, String name, int species) throws SQLException {
		int id = this.getid("sample");
		// int id1= this.getid("dataset_sample");

		PreparedStatement prep1 = null;
		String query1 = "insert into sample(id,species_id, name) values(?,?,?)";

		System.out.println(query1);
		prep1 = con.prepareStatement(query1);
		prep1.setInt(1, id);
		prep1.setInt(2, species);
		prep1.setString(3, name);

		prep1.executeUpdate();

		String query2 = "insert into dataset_sample(dataset_id, sample_id) values(?,?)";

		System.out.println(query2);
		prep1 = con.prepareStatement(query2);
		prep1.setInt(1, datasetid);
		prep1.setInt(2, id);
		prep1.executeUpdate();

	}

	public void addsample_attribute(int sample_id, int attribute_id, String value, String unit_id) throws SQLException {

		PreparedStatement prep1 = null;
		String query1 = "insert into sample_attribute(sample_id, attribute_id, value, unit_id) values(?,?,?,?)";

		System.out.println(query1);
		prep1 = con.prepareStatement(query1);
		prep1.setInt(1, sample_id);
		prep1.setInt(2, attribute_id);
		prep1.setString(3, value);
		prep1.setString(4, unit_id);
		prep1.executeUpdate();

	}

	public void addsample_attribute_without_unit(int sample_id, int attribute_id, String value) throws SQLException {

		PreparedStatement prep1 = null;
		String query1 = "insert into sample_attribute(sample_id, attribute_id, value) values(?,?,?)";
		value = value.replaceAll("\u0000", "");
		System.out.println(query1);
		prep1 = con.prepareStatement(query1);
		prep1.setInt(1, sample_id);
		prep1.setInt(2, attribute_id);
		prep1.setString(3, value);

		prep1.executeUpdate();

	}

	public void close() throws SQLException {
		con.close();

	}

	public static void main(String[] args) throws Exception {
		database_v1 db = new database_v1();
	}

}
