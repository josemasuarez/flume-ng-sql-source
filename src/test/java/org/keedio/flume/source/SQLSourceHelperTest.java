package org.keedio.flume.source;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marcelo Valle https://github.com/mvalleavila
 */

// @RunWith(PowerMockRunner.class)
public class SQLSourceHelperTest {

	Context context = mock(Context.class);

	@Before
	public void setup() {

		when(context.getString("status.file.name")).thenReturn("statusFileName.txt");
		when(context.getString("hibernate.connection.url")).thenReturn("jdbc:oracle:thin:@localhost:1521:XE");
		// when(context.getString("table")).thenReturn("table");
		when(context.getString("custom.query")).thenReturn(
				"SELECT * FROM (select TO_NUMBER(TO_CHAR(TO_TIMESTAMP (time_stamp, 'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDDHHMISS')) AS INCREMENTAL, OE.WLSLOG.* from OE.WLSLOG) WHERE INCREMENTAL > $@$ ORDER BY INCREMENTAL ASC");
		when(context.getString("incremental.column.name")).thenReturn("INCREMENTAL");
		when(context.getString("status.file.path", "/var/lib/flume")).thenReturn("/tmp/flume");
		// when(context.getString("columns.to.select", "*")).thenReturn("*");
		when(context.getInteger("run.query.delay", 10000)).thenReturn(10000);
		when(context.getInteger("batch.size", 100)).thenReturn(100);
		when(context.getInteger("max.rows", 10000)).thenReturn(10000);
		when(context.getString("incremental.value", "0")).thenReturn("0");
		when(context.getString("start.from", "0")).thenReturn("0");
	}

	@Test
	public void getConnectionURL() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		assertEquals("jdbc:oracle:thin:@localhost:1521:XE", sqlSourceHelper.getConnectionURL());
	}

	@Test
	public void getCurrentIndex() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		assertEquals("0", sqlSourceHelper.getCurrentIndex());
	}

	@Test
	public void setCurrentIndex() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		sqlSourceHelper.setCurrentIndex("10");
		assertEquals("10", sqlSourceHelper.getCurrentIndex());
	}

	@Test
	public void getRunQueryDelay() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		assertEquals(10000, sqlSourceHelper.getRunQueryDelay());
	}

	@Test
	public void getBatchSize() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		assertEquals(100, sqlSourceHelper.getBatchSize());
	}

	@Test
	public void getCustomQuery() {
		when(context.getString("custom.query")).thenReturn("SELECT column FROM table");
		when(context.getString("incremental.column")).thenReturn("incremental");
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		assertEquals("SELECT column FROM table", sqlSourceHelper.getQuery());
	}

	@Test
	public void chekGetAllRowsWithNullParam() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		assertEquals(new ArrayList<String>(), sqlSourceHelper.getAllRows(null));
	}

	@Test(expected = ConfigurationException.class)
	public void checkStatusFileNameNotSet() {
		when(context.getString("status.file.name")).thenReturn(null);
		new SQLSourceHelper(context, "Source Name");
	}

	@Test(expected = ConfigurationException.class)
	public void connectionURLNotSet() {
		when(context.getString("hibernate.connection.url")).thenReturn(null);
		new SQLSourceHelper(context, "Source Name");
	}

	@Test(expected = ConfigurationException.class)
	public void customQueryNotSet() {
		when(context.getString("custom.query")).thenReturn(null);
		new SQLSourceHelper(context, "Source Name");
	}

	@Test
	public void chekGetAllRowsWithEmptyParam() {

		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		assertEquals(new ArrayList<String>(), sqlSourceHelper.getAllRows(new ArrayList<List<Object>>()));
	}

	@SuppressWarnings("deprecation")
	@Test
	public void chekGetAllRows() {

		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		List<List<Object>> queryResult = new ArrayList<List<Object>>(2);
		List<String[]> expectedResult = new ArrayList<String[]>(2);
		String string1 = "string1";
		String string2 = "string2";
		int int1 = 1;
		int int2 = 2;
		Date date1 = new Date(115, 0, 1);
		Date date2 = new Date(115, 1, 2);

		List<Object> row1 = new ArrayList<Object>(3);
		String[] expectedRow1 = new String[3];
		row1.add(string1);
		expectedRow1[0] = string1;
		row1.add(int1);
		expectedRow1[1] = Integer.toString(int1);
		row1.add(date1);
		expectedRow1[2] = date1.toString();
		queryResult.add(row1);
		expectedResult.add(expectedRow1);

		List<Object> row2 = new ArrayList<Object>(3);
		String[] expectedRow2 = new String[3];
		row2.add(string2);
		expectedRow2[0] = string2;
		row2.add(int2);
		expectedRow2[1] = Integer.toString(int2);
		row2.add(date2);
		expectedRow2[2] = date2.toString();
		queryResult.add(row2);
		expectedResult.add(expectedRow2);

		assertArrayEquals(expectedResult.get(0), sqlSourceHelper.getAllRows(queryResult).get(0));
		assertArrayEquals(expectedResult.get(1), sqlSourceHelper.getAllRows(queryResult).get(1));
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void chekGetJsonAllRows() {

		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		List<Map<String,Object>> queryResult = new ArrayList<Map<String,Object>>();
		String string1 = "string1";
		
		int int1 = 1;
		

		Map<String,Object> row1 = new HashMap<String,Object>();
		row1.put("column1", string1);
		row1.put("column2",Integer.toString(int1));
		row1.put("column3",Integer.toString(int1));
		queryResult.add(row1);
		
		JsonArray jsonArray = sqlSourceHelper.getAllRowsToJsonArray(queryResult);
		jsonArray.get(0).toString();
	}

	@SuppressWarnings("unused")
	@Test
	public void createDirectory() {

		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		File file = new File("/tmp/flume");
		assertEquals(true, file.exists());
		assertEquals(true, file.isDirectory());
		if (file.exists()) {
			file.delete();
		}
	}

	@Test
	public void checkStatusFileCorrectlyCreated() {

		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		// sqlSourceHelper.setCurrentIndex(10);

		sqlSourceHelper.updateStatusFile();

		File file = new File("/tmp/flume/statusFileName.txt");
		assertEquals(true, file.exists());
		if (file.exists()) {
			file.delete();
			file.getParentFile().delete();
		}
	}

	@Test
	public void checkStatusFileCorrectlyUpdated() throws Exception {

		// File file = File.createTempFile("statusFileName", ".txt");

		when(context.getString("status.file.path")).thenReturn("/var/lib/flume");
		when(context.getString("hibernate.connection.url")).thenReturn("jdbc:oracle:thin:@localhost:1521:XE");
		when(context.getString("table")).thenReturn("table");
		when(context.getString("status.file.name")).thenReturn("statusFileName");

		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, "Source Name");
		sqlSourceHelper.createStatusFile();
		sqlSourceHelper.setCurrentIndex("10");

		sqlSourceHelper.updateStatusFile();

		SQLSourceHelper sqlSourceHelper2 = new SQLSourceHelper(context, "Source Name");
		assertEquals("10", sqlSourceHelper2.getCurrentIndex());
	}

	@After
	public void deleteDirectory() {
		try {

			File file = new File("/tmp/flume");
			if (file.exists())
				FileUtils.deleteDirectory(file);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
