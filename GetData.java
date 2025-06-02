import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.HashMap;

import org.json.JSONObject;
import org.json.JSONArray;

public class GetData {

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database
    String userTableName = null;
    String friendsTableName = null;
    String cityTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;

    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
        super();
        String dataType = u;
        oracleConnection = c;
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        cityTableName = prefix + dataType + "_CITIES";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITIES";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITIES";
    }

    // TODO: Implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException {

        // This is the data structure to store all users' information
        JSONArray users_info = new JSONArray();

        Map<Integer, JSONObject> hometownmapping = new HashMap<>();
        Map<Integer, JSONObject> currentCityMapping = new HashMap<>();
   

        
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs;

            String userQuerying = String.format(
                "SELECT user_id, first_name, last_name, gender, year_of_birth, month_of_birth, day_of_birth " +
                "FROM %s ORDER BY user_id", userTableName
            );

            String friendsQuerying = String.format("SELECT user1_id, user2_id FROM %s", friendsTableName);
            rs = stmt.executeQuery(friendsQuerying);
            Map<Integer, JSONArray> friendsMapping = new HashMap<>();
            while (rs.next()) {
                int u1 = rs.getInt("user1_id");
                int u2 = rs.getInt("user2_id");

                int smaller = Math.min(u1, u2);
                int larger = Math.max(u1, u2);

                if (!friendsMapping.containsKey(smaller)) {
                    friendsMapping.put(smaller, new JSONArray());
                }
                friendsMapping.get(smaller).put(larger);
            }
            rs.close();


            String currentCityQuerying = String.format(
                "SELECT u.user_id, c.city_id, c.city_name, c.state_name, c.country_name " +
                "FROM %s u " +
                "JOIN %s ucc ON u.user_id = ucc.user_id " +
                "JOIN %s c ON ucc.current_city_id = c.city_id",
                userTableName, currentCityTableName, cityTableName
            );

            ResultSet rs1 = stmt.executeQuery(currentCityQuerying);
            while (rs1.next()) {
                JSONObject currentCity = new JSONObject();
                currentCity.put("city", rs1.getString("city_name"));
                currentCity.put("state", rs1.getString("state_name"));
                currentCity.put("country", rs1.getString("country_name"));

                int userId = rs1.getInt("user_id");
                if (!currentCityMapping.containsKey(userId)) {
                    currentCityMapping.put(userId, currentCity);
                }
            }
            rs1.close();

            String hometownQuerying = String.format(
                "SELECT u.user_id, c.city_id, c.city_name, c.state_name, c.country_name " +
                "FROM %s u " +
                "JOIN %s uhc ON u.user_id = uhc.user_id " +
                "JOIN %s c ON uhc.hometown_city_id = c.city_id",
                userTableName, hometownCityTableName, cityTableName
            );

            ResultSet rs2 = stmt.executeQuery(hometownQuerying);
            while (rs2.next()) {
                JSONObject hometownJson = new JSONObject();
                hometownJson.put("city", rs2.getString("city_name"));
                hometownJson.put("state", rs2.getString("state_name"));
                hometownJson.put("country", rs2.getString("country_name"));

                int userId = rs2.getInt("user_id");
                if (!hometownmapping.containsKey(userId)) {
                    hometownmapping.put(userId, hometownJson);
                }
            }
            rs2.close();

            ResultSet rs3 = stmt.executeQuery(userQuerying);
            while (rs3.next()) {
                JSONObject user_json = new JSONObject();

                user_json.put("user_id", rs3.getInt("user_id"));
                user_json.put("first_name", rs3.getString("first_name"));
                user_json.put("last_name", rs3.getString("last_name"));
                user_json.put("gender", rs3.getString("gender"));
                user_json.put("YOB", rs3.getInt("year_of_birth"));
                user_json.put("MOB", rs3.getInt("month_of_birth"));
                user_json.put("DOB", rs3.getInt("day_of_birth"));

                int userId = rs3.getInt("user_id");

                user_json.put("friends", friendsMapping.getOrDefault(userId, new JSONArray()));
                user_json.put("current", currentCityMapping.getOrDefault(userId, new JSONObject()));
                user_json.put("hometown", hometownmapping.getOrDefault(userId, new JSONObject()));

                users_info.put(user_json);
            }
            rs3.close();

            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return users_info;
    }

    // This outputs to a file "output.json"
    // DO NOT MODIFY this function
    public void writeJSON(JSONArray users_info) {
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir") + "/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
