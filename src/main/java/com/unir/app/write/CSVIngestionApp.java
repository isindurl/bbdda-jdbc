package com.unir.app.write;

import com.opencsv.CSVReader;
import com.unir.config.MySqlConnector;
import lombok.extern.slf4j.Slf4j;

import java.io.FileReader;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

@Slf4j
public class CSVIngestionApp {

    private static final String DB_NAME = "employees";
    private static final String DEPARTMENTS_CSV = "C:\\Users\\Usuario\\BBDDA_WORKSPACE\\BBDDA_WORKSPACE\\unirDepartmentsNew.csv";
    private static final String EMPLOYEES_CSV = "C:\\Users\\Usuario\\BBDDA_WORKSPACE\\BBDDA_WORKSPACE\\unirEmployeesNew.csv";
    private static final String DEPT_EMP_CSV = "C:\\Users\\Usuario\\BBDDA_WORKSPACE\\BBDDA_WORKSPACE\\unirNewDeptEmp.csv";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

    public static void main(String[] args) {
        try (Connection connection = new MySqlConnector("localhost", DB_NAME).getConnection()) {
            connection.setAutoCommit(false);
            processDepartmentsCSV(connection, DEPARTMENTS_CSV);
            processEmployeesCSV(connection, EMPLOYEES_CSV);
            processDeptEmpCSV(connection, DEPT_EMP_CSV);
            connection.commit();
            System.out.println("Ingesta completada exitosamente.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processDepartmentsCSV(Connection connection, String filePath) throws SQLException {
        String selectSql = "SELECT COUNT(*) FROM departments WHERE dept_no = ?";
        String insertSql = "INSERT INTO departments (dept_no, dept_name) VALUES (?, ?)";
        String updateSql = "UPDATE departments SET dept_name = ? WHERE dept_no = ?";

        try (CSVReader reader = new CSVReader(new FileReader(filePath));
             PreparedStatement selectStmt = connection.prepareStatement(selectSql);
             PreparedStatement insertStmt = connection.prepareStatement(insertSql);
             PreparedStatement updateStmt = connection.prepareStatement(updateSql)) {

            List<String[]> batchData = reader.readAll();
            batchData.remove(0); // Elimina la cabecera

            for (String[] record : batchData) {
                String deptNo = record[0];
                String deptName = record[1];

                selectStmt.setString(1, deptNo);
                ResultSet rs = selectStmt.executeQuery();
                rs.next();

                if (rs.getInt(1) > 0) {
                    updateStmt.setString(1, deptName);
                    updateStmt.setString(2, deptNo);
                    updateStmt.addBatch();
                } else {
                    insertStmt.setString(1, deptNo);
                    insertStmt.setString(2, deptName);
                    insertStmt.addBatch();
                }
            }

            insertStmt.executeBatch();
            updateStmt.executeBatch();
            System.out.println("Departamentos procesados.");
        } catch (Exception e) {
            connection.rollback();
            e.printStackTrace();
        }
    }

    private static void processEmployeesCSV(Connection connection, String filePath) throws SQLException {
        String selectSql = "SELECT COUNT(*) FROM employees WHERE emp_no = ?";
        String insertSql = "INSERT INTO employees (emp_no, first_name, last_name, gender, hire_date, birth_date) VALUES (?, ?, ?, ?, ?, ?)";
        String updateSql = "UPDATE employees SET first_name = ?, last_name = ?, gender = ?, hire_date = ?, birth_date = ? WHERE emp_no = ?";

        try (CSVReader reader = new CSVReader(new FileReader(filePath));
             PreparedStatement selectStmt = connection.prepareStatement(selectSql);
             PreparedStatement insertStmt = connection.prepareStatement(insertSql);
             PreparedStatement updateStmt = connection.prepareStatement(updateSql)) {

            List<String[]> batchData = reader.readAll();
            batchData.remove(0); // Elimina la cabecera

            for (String[] record : batchData) {
                int empNo = Integer.parseInt(record[0]);
                String firstName = record[1];
                String lastName = record[2];
                String gender = record[3];
                Date hireDate = parseDate(record[4]);
                Date birthDate = parseDate(record[5]);

                selectStmt.setInt(1, empNo);
                ResultSet rs = selectStmt.executeQuery();
                rs.next();

                if (rs.getInt(1) > 0) {
                    updateStmt.setString(1, firstName);
                    updateStmt.setString(2, lastName);
                    updateStmt.setString(3, gender);
                    updateStmt.setDate(4, hireDate);
                    updateStmt.setDate(5, birthDate);
                    updateStmt.setInt(6, empNo);
                    updateStmt.addBatch();
                } else {
                    insertStmt.setInt(1, empNo);
                    insertStmt.setString(2, firstName);
                    insertStmt.setString(3, lastName);
                    insertStmt.setString(4, gender);
                    insertStmt.setDate(5, hireDate);
                    insertStmt.setDate(6, birthDate);
                    insertStmt.addBatch();
                }
            }

            insertStmt.executeBatch();
            updateStmt.executeBatch();
            System.out.println("Empleados procesados.");
        } catch (Exception e) {
            connection.rollback();
            e.printStackTrace();
        }
    }

    private static void processDeptEmpCSV(Connection connection, String filePath) throws SQLException {
        String selectEmployeeSql = "SELECT COUNT(*) FROM employees WHERE emp_no = ?";
        String selectDeptEmpSql = "SELECT COUNT(*) FROM dept_emp WHERE emp_no = ? AND dept_no = ?";
        String insertSql = "INSERT INTO dept_emp (emp_no, dept_no, from_date, to_date) VALUES (?, ?, ?, ?)";
        String updateSql = "UPDATE dept_emp SET from_date = ?, to_date = ? WHERE emp_no = ? AND dept_no = ?";

        try (CSVReader reader = new CSVReader(new FileReader(filePath));
             PreparedStatement selectEmpStmt = connection.prepareStatement(selectEmployeeSql);
             PreparedStatement selectDeptEmpStmt = connection.prepareStatement(selectDeptEmpSql);
             PreparedStatement insertStmt = connection.prepareStatement(insertSql);
             PreparedStatement updateStmt = connection.prepareStatement(updateSql)) {

            List<String[]> batchData = reader.readAll();
            batchData.remove(0); // Elimina la cabecera

            for (String[] record : batchData) {
                int empNo = Integer.parseInt(record[0]);
                String deptNo = record[1];
                Date fromDate = parseDate(record[2]);
                Date toDate = parseDate(record[3]);

                selectEmpStmt.setInt(1, empNo);
                ResultSet empRs = selectEmpStmt.executeQuery();
                empRs.next();
                if (empRs.getInt(1) == 0) {
                    System.err.println("El empleado con emp_no " + empNo + " no existe en employees. Saltando.");
                    continue;
                }

                selectDeptEmpStmt.setInt(1, empNo);
                selectDeptEmpStmt.setString(2, deptNo);
                ResultSet rs = selectDeptEmpStmt.executeQuery();
                rs.next();

                if (rs.getInt(1) > 0) {
                    updateStmt.setDate(1, fromDate);
                    updateStmt.setDate(2, toDate);
                    updateStmt.setInt(3, empNo);
                    updateStmt.setString(4, deptNo);
                    updateStmt.addBatch();
                } else {
                    insertStmt.setInt(1, empNo);
                    insertStmt.setString(2, deptNo);
                    insertStmt.setDate(3, fromDate);
                    insertStmt.setDate(4, toDate);
                    insertStmt.addBatch();
                }
            }

            insertStmt.executeBatch();
            updateStmt.executeBatch();
            System.out.println("Relaciones empleado-departamento procesadas.");
        } catch (Exception e) {
            connection.rollback();
            e.printStackTrace();
        }
    }

    private static Date parseDate(String dateStr) {
        try {
            java.util.Date parsedDate = DATE_FORMAT.parse(dateStr);
            return new Date(parsedDate.getTime());
        } catch (ParseException e) {
            System.err.println("Error al analizar la fecha: " + dateStr);
            return null;
        }
    }
}
