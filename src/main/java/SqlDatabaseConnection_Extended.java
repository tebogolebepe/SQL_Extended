import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

public class  SqlDatabaseConnection_Extended{

    private static ResultSet resultSet;
    private static PreparedStatement preparedStatement;

    private static Logger logger = LogManager.getLogger(SqlDatabaseConnection_Extended.class.getName());
    public static void main(String[] args) {
        try {


            Class.forName("org.postgresql.Driver");

            Connection connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/umuzi", "user", "pass");

            Statement statement = connection.createStatement();

            resultSet = statement.executeQuery("select * from customers");
            logger.info("SELECT ALL records from table Customers");
            while (resultSet.next()) {
                System.out.format("%2s %9s %10s %8s %20s %13s %20s %14s %14s\n",
                        resultSet.getInt("customerid"),
                        resultSet.getString("firstname"),
                        resultSet.getString("lastname"),
                        resultSet.getString("gender"),
                        resultSet.getString("address"),
                        resultSet.getString("phone"),
                        resultSet.getString("email"),
                        resultSet.getString("city"),
                        resultSet.getString("country"));
            }

            logger.info("SELECT records only from the name column in the Customers table");
            resultSet = statement.executeQuery("SELECT firstname,lastname FROM Customers");
            while (resultSet.next()) {
                System.out.format("%2s %10s\n",
                        resultSet.getString("firstname"),
                        resultSet.getString("lastname"));
            }

            logger.info("Show the name of the Customer whose CustomerID is 1");
            resultSet = statement.executeQuery("SELECT firstname FROM Customers WHERE customerid = 1");
            while (resultSet.next()) {
                System.out.format("%s\n", resultSet.getString("firstname"));
            }

            String queryUpdate = "UPDATE customers SET firstname = ?, lastname = ? WHERE customerid = ?";
            preparedStatement = connection.prepareStatement(queryUpdate);
            preparedStatement.setString(1, "Lerato");
            preparedStatement.setString(2, "Mabitso");
            preparedStatement.setInt(3, 1);

            preparedStatement.executeUpdate();

            logger.debug("Successfully Updated!!");

            String queryDelete = "DELETE FROM customers WHERE customerid = ?";
            preparedStatement = connection.prepareStatement(queryDelete);
            preparedStatement.setInt(1, 2);

            /* execute the preparedStatement*/
            preparedStatement.execute();
            logger.debug("Successfully deleted the records from database!!");

            resultSet = statement.executeQuery("select * from orders");
            logger.info("SELECT ALL records from table orders");
            while (resultSet.next()) {
                System.out.format("%2s %9s %10s %8s %20s %13s %20s\n",
                        resultSet.getInt("orderid"),
                        resultSet.getInt("productid"),
                        resultSet.getInt("paymentid"),
                        resultSet.getInt("fulfilledbyemployeeid"),
                        resultSet.getString("daterequired"),
                        resultSet.getString("dateshipped"),
                        resultSet.getString("status"));
            }

            String queryCount = "SELECT COUNT(DISTINCT status) FROM orders";

            resultSet = statement.executeQuery(queryCount);

            resultSet.next();
            int count = resultSet.getInt(1);

            System.out.println("Count for all numbers : " + count);



            resultSet = statement.executeQuery("select * from payments");
            logger.info("SELECT ALL records from table payments");
            while (resultSet.next()) {
                System.out.format("%2s %9s %10s %8s \n",
                        resultSet.getInt("paymentid"),
                        resultSet.getInt("customerid"),
                        resultSet.getString("paymentdate"),
                        resultSet.getString("amount"));
            }
            String queryMax = "SELECT MAX (amount) FROM payments";

            resultSet = statement.executeQuery(queryMax);

            resultSet.next();
            double price = resultSet.getDouble(1);

            System.out.println("maximum payment: " + price);

            connection.close();


        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }
}