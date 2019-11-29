package sample_update;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.xml.sax.SAXException;

public class sample {


	public static void main(String[] args) throws SQLException, SAXException, IOException, ParserConfigurationException

	{
		
		Setting.Loadsetting();
		String filepath= Setting.path;
		String doi = Setting.doi; 
		int species_id = 0;
		database_v1 db = new database_v1();
		int datasetid = db.getdataset_id(doi);
		String sample_id = null;
		String name = null;
		System.out.println(doi);
		System.out.println(filepath);
		File file = new File(filepath);
		if (!file.isFile()) {
			throw new RuntimeException(file + "xxx");
		}
		POIFSFileSystem fs = null;
		HSSFWorkbook wb = null;
		HSSFSheet sheet = null;
		try {
			fs = new POIFSFileSystem(new FileInputStream(file));
			wb = new HSSFWorkbook(fs);
			sheet = wb.getSheetAt(0);
		} catch (IOException e) {
			System.out.println(file);
			e.printStackTrace();
		}
		db.delete_sample();
		int id = db.getid("sample");
		ArrayList<Integer> attributes_id = new ArrayList<Integer>();

		for (Row row : sheet) {
			System.out.println(" ");
			System.out.println(" ");
			// System.out.println(row.getRowNum());
			if (row.getRowNum() == 0) {
				for (Cell cell : row) {
					int columnIndex = cell.getColumnIndex();
					if (columnIndex > 2) {
						String value = cell.getStringCellValue();

						// System.out.println("one"+ value);
						int att_id = db.getattribute_id(value);
						if (att_id == 0) {

							att_id = db.add_attribute(value);
							System.out.println("！！！！！！！！！！！！！！！new add: " + value + " " + att_id);

						}
						attributes_id.add(att_id);
					}

				}
				System.out.println(attributes_id);

			} else {

				for (Cell cell : row) {

					int columnIndex = cell.getColumnIndex();
					if (columnIndex == 0) {
						if (cell.getCellType() == 0) {
							sample_id = String.valueOf(cell.getNumericCellValue());
							if (sample_id.endsWith(".0")) {
								sample_id = sample_id.replace(".0", "");
							}

							System.out.println(sample_id);

						} else {
							sample_id = cell.getStringCellValue();

							System.out.println(sample_id);
						}

					}
					if (columnIndex == 1) {

						species_id = (int) cell.getNumericCellValue();

						System.out.println(species_id);
						species_id = db.getspecies_id(species_id);

						if (species_id == 0) {
							System.out.println("no species: " + row.getCell(2).getStringCellValue());
							species_id = db.addspeciestable((int) row.getCell(1).getNumericCellValue(),
									row.getCell(2).getStringCellValue());
						}

						System.out.println(species_id);
						db.addsamplev3(datasetid, sample_id, species_id);

					}
					if (columnIndex == 2) {

						name = cell.getStringCellValue();

						System.out.println(name);

					}

					if (columnIndex > 2) {

						String value = "";
						if (cell.getCellType() == 0) {
							value = String.valueOf(cell.getNumericCellValue());
							if (value.endsWith(".0")) {
								value = value.replace(".0", "");
							}

							System.out.println(value);

						} else {
							value = cell.getStringCellValue();
							System.out.println(value);
						}

						if (value.contains("::")) {
							System.out.println(attributes_id.get(columnIndex - 3) + " contain unit " + value);
							String[] valueunit = value.split("::");
							String unitid = db.getunit_id(valueunit[1]);
							db.addsample_attribute(id, attributes_id.get(columnIndex - 3), valueunit[0], unitid);

						} else if (value == "" || value.isEmpty() || value == null) {
							continue;
						} else {
							db.addsample_attribute_without_unit(id, attributes_id.get(columnIndex - 3), value);
						}

					}

				}
				
				id++;

			}

			// db.updatesampleattribute(sample_id, attribute,doi);

		}
		db.updateids();
		db.close();

	}
}
