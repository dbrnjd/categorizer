package src; // Make sure this package declaration is at the top

// Import necessary libraries
import org.json.JSONArray; // For creating JSON arrays
import org.json.JSONObject; // For creating JSON objects

import java.io.BufferedReader; // For reading text efficiently
import java.io.BufferedWriter; // For writing text efficiently
import java.io.IOException; // For handling input/output exceptions
import java.io.InputStreamReader; // To bridge byte streams to character streams for reading
import java.io.OutputStreamWriter; // To bridge byte streams to character streams for writing

import java.util.ArrayList; // For dynamic lists
import java.util.Arrays; // For array manipulation (used in sorting)
import java.util.HashMap; // For key-value maps
import java.util.List; // Interface for lists
import java.util.Map; // Interface for maps

// Import necessary Hadoop classes for HDFS interaction
import org.apache.hadoop.conf.Configuration; // For Hadoop configuration
import org.apache.hadoop.fs.FileSystem; // For interacting with Hadoop FileSystem (HDFS)
import org.apache.hadoop.fs.Path; // Represents a path in HDFS
import org.apache.hadoop.fs.FSDataInputStream; // Input stream for HDFS files
import org.apache.hadoop.fs.FSDataOutputStream; // Output stream for HDFS files

/**
 * Processes patient data from a CSV file on HDFS, classifies it,
 * and outputs the results to JSON files on HDFS.
 */
public class PatientDataProcessor {

    /**
     * Main method - the entry point of the application.
     * Expected command-line arguments: <input_csv_path> <output_directory_path>
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        // Debug print statement to verify arguments received by the Java program
        System.out.println("DEBUG: First argument received: " + (args.length > 0 ? args[0] : "No arguments provided"));

        // Check if the correct number of command-line arguments are provided
        if (args.length < 2) {
            System.err.println("Usage: PatientDataProcessor <input_csv_path> <output_directory_path>");
            System.exit(1); // Exit if arguments are missing
        }

        // Extract input and output paths from command-line arguments
        String inputCsvPathString = args[0]; // First argument is the input CSV HDFS path
        String outputDirPathString = args[1]; // Second argument is the output directory HDFS path

        List<Patient> patientList = new ArrayList<>(); // List to hold Patient objects

        // --- Read data from HDFS using Hadoop FileSystem API ---
        Configuration conf = new Configuration(); // Create a Hadoop configuration object
        FileSystem fs = null; // FileSystem object for HDFS interaction
        BufferedReader br = null; // BufferedReader to read the CSV file

        try {
            // Get the HDFS FileSystem instance based on the configuration
            fs = FileSystem.get(conf);

            // Create a Hadoop Path object from the input path string
            Path inputCsvPath = new Path(inputCsvPathString);

            // Open the input file from HDFS, returning an FSDataInputStream
            FSDataInputStream inputStream = fs.open(inputCsvPath);

            // Wrap the HDFS input stream with InputStreamReader and BufferedReader
            // to read the file line by line as text.
            br = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            br.readLine(); // Read and discard the header line (assuming CSV has a header)

            // Read each line of the CSV file
            while ((line = br.readLine()) != null) {
                String[] fields = parseCsvLine(line); // Parse the CSV line into fields
                if (fields.length > 0) {
                    // Create a Patient object from the parsed fields
                    Patient patient = createPatientFromFields(fields);
                    patientList.add(patient); // Add the patient to the list
                } else {
                    System.out.println("Skipping invalid line: " + line); // Log invalid lines
                }
            }

            // --- Perform classification and output results to HDFS as JSON ---
            // Pass the list of patients, the output directory path, and the FileSystem object
            classifyAndOutputPatientsToJson(patientList, outputDirPathString, fs);

        } catch (IOException e) {
            // Catch and print any IOException that occurs during file reading or HDFS operations
            e.printStackTrace();
        } finally {
            // Ensure resources are closed properly in the finally block
            try {
                if (br != null) br.close(); // Close the BufferedReader
                if (fs != null) fs.close(); // Close the FileSystem connection
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Parses a single line of the CSV, handling quoted fields and commas within quotes.
     * @param line The CSV line to parse.
     * @return An array of strings, where each element is a field from the CSV line.
     */
    private static String[] parseCsvLine(String line) {
        List<String> values = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentValue = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                // Handle escaped quotes ("") within a quoted field
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    currentValue.append('"');
                    i++; // Skip the next quote as it's part of an escaped pair
                } else {
                    inQuotes = !inQuotes; // Toggle inQuotes state
                }
            } else if (c == ',' && !inQuotes) {
                // If a comma is found outside of quotes, it's a field delimiter
                values.add(currentValue.toString().trim()); // Add the current value to the list (trimmed)
                currentValue = new StringBuilder(); // Reset for the next value
            } else if (c == '\r' || c == '\n') {
                // Handle line breaks potentially within quoted fields
                if (inQuotes) {
                    currentValue.append(c); // Include line break if inside quotes
                }
                // If not in quotes, line break signals the end of the line, but the outer loop handles this.
            } else {
                // Append any other character to the current value
                currentValue.append(c);
            }
        }

        // Add the last value after the loop finishes
        values.add(currentValue.toString().trim());
        return values.toArray(new String[0]); // Convert the list of values to a string array
    }

    /**
     * Creates a Patient object from an array of parsed CSV fields.
     * @param fields The array of strings representing the CSV fields for one patient.
     * @return A new Patient object populated with data from the fields.
     */
    private static Patient createPatientFromFields(String[] fields) {
        Patient patient = new Patient();

        // Assign fields to Patient object attributes based on expected CSV column order
        // (Adjust index numbers based on your specific CSV file's column order)
        if (fields.length > 0) {
            patient.setPatientId(fields[0].trim());
        }
        if (fields.length > 1) {
            patient.setName(fields[1].trim());
        }
        if (fields.length > 2) {
            try {
                patient.setAge(Integer.parseInt(fields[2].trim()));
            } catch (NumberFormatException e) {
                System.err.println("Warning: Could not parse age for patient ID " + patient.getPatientId() + ". Value: " + fields[2]);
                patient.setAge(0); // Set a default or handle error appropriately
            }
        }
        if (fields.length > 3) {
            patient.setGender(fields[3].trim());
        }
        if (fields.length > 4) {
            patient.setRegion(fields[4].trim());
        }
        if (fields.length > 5) {
            patient.setSymptoms(fields[5].trim()); // Assuming symptoms are in the 6th column (index 5)
        }
        // Add more fields as needed based on your CSV structure

        return patient;
    }

    /**
     * Classifies patients based on different criteria (ID, Region, Symptom)
     * and outputs the classified data as JSON files to the specified HDFS directory.
     * @param patientList The list of all Patient objects.
     * @param outputDirPathString The HDFS path string for the output directory.
     * @param fs The Hadoop FileSystem object for HDFS operations.
     */
    private static void classifyAndOutputPatientsToJson(List<Patient> patientList, String outputDirPathString, FileSystem fs) {
        // Maps to store classified patients
        Map<String, Patient> patientIdMap = new HashMap<>(); // Maps Patient ID to Patient object
        Map<String, List<Patient>> regionMap = new HashMap<>(); // Maps Region to a list of Patients in that region
        Map<String, List<Patient>> symptomMap = new HashMap<>(); // Maps Normalized Symptom string to a list of Patients with that symptom

        // Map to store a representative original symptom for each normalized symptom key
        Map<String, String> normalizedToOriginalSymptom = new HashMap<>();

        // --- Classification Logic ---
        for (Patient patient : patientList) {
            // Classify by Patient ID (simple map)
            patientIdMap.put(patient.getPatientId(), patient);

            // Classify by Region
            String region = patient.getRegion();
            // If the region is not yet a key in the map, add it with a new list
            if (!regionMap.containsKey(region)) {
                regionMap.put(region, new ArrayList<>());
            }
            regionMap.get(region).add(patient); // Add the patient to the list for their region

            // Classify by Symptom (using normalized symptom as the key)
            String originalSymptom = patient.getSymptoms(); // Get the original symptom string
            if (originalSymptom != null && !originalSymptom.isEmpty()) {
                String normalizedSymptom = normalizeSymptom(originalSymptom); // Normalize the symptom string

                // If this normalized symptom is not yet a key in the map, add it
                if (!symptomMap.containsKey(normalizedSymptom)) {
                    symptomMap.put(normalizedSymptom, new ArrayList<>());
                    // Store the original symptom string for this normalized key
                    normalizedToOriginalSymptom.put(normalizedSymptom, originalSymptom);
                }
                // Add the patient to the list for this normalized symptom
                symptomMap.get(normalizedSymptom).add(patient);
            }
        }

        // --- Output Classified Data to JSON Files on HDFS ---
        try {
            // Create a Hadoop Path object for the output directory string
            Path outputDirPath = new Path(outputDirPathString);

            // Create the output directory in HDFS if it doesn't exist
            if (!fs.exists(outputDirPath)) {
                fs.mkdirs(outputDirPath);
            }

            // Construct full HDFS paths for the output JSON files
            Path patientIdOutputPath = new Path(outputDirPath, "patient_id_output.json");
            Path regionOutputPath = new Path(outputDirPath, "region_output.json");
            Path symptomOutputPath = new Path(outputDirPath, "symptom_output.json");

            // Write the classified data to the respective JSON files on HDFS
            writePatientIdOutputToJson(patientIdMap, patientIdOutputPath, fs);
            writeRegionOutputToJson(regionMap, regionOutputPath, fs);
            writeSymptomOutputToJson(symptomMap, normalizedToOriginalSymptom, symptomOutputPath, fs);

        } catch (IOException e) {
            // Catch and print any IOException during output file creation or writing
            e.printStackTrace();
        }
    }

    /**
     * Helper method to normalize a symptom string for consistent grouping.
     * Converts to lowercase, removes punctuation, splits into words, sorts words, and joins them.
     * @param symptom The original symptom string.
     * @return The normalized symptom string.
     */
    private static String normalizeSymptom(String symptom) {
        // Convert the entire symptom string to lowercase
        String normalized = symptom.toLowerCase();
        // Remove punctuation and non-alphanumeric characters (except spaces)
        normalized = normalized.replaceAll("[^a-z0-9\\s]", "");
        // Split the normalized string into individual words using whitespace as delimiter
        String[] words = normalized.split("\\s+");
        // Sort the array of words alphabetically
        Arrays.sort(words);
        // Join the sorted words back into a single string, separated by spaces
        return String.join(" ", words);
    }


    /**
     * Writes the patient data mapped by ID to a JSON array on HDFS.
     * Output format: [{"patientId": "...", "patient": {...}}, ...]
     * @param patientIdMap Map of Patient ID to Patient object.
     * @param outputPath The HDFS Path where the JSON file will be written.
     * @param fs The Hadoop FileSystem object.
     * @throws IOException If an I/O error occurs.
     */
    private static void writePatientIdOutputToJson(Map<String, Patient> patientIdMap, Path outputPath, FileSystem fs) throws IOException {
        JSONArray jsonArray = new JSONArray(); // Create a new JSON array

        // Iterate over the entries in the patientIdMap
        for (Map.Entry<String, Patient> entry : patientIdMap.entrySet()) {
            JSONObject jsonObject = new JSONObject(); // Create a JSON object for each entry
            jsonObject.put("patientId", entry.getKey()); // Put the patient ID as a field
            jsonObject.put("patient", patientToJson(entry.getValue())); // Put the Patient object (converted to JSON) as another field
            jsonArray.put(jsonObject); // Add the object to the JSON array
        }

        // Write the JSON array to the specified HDFS file path
        FSDataOutputStream outputStream = fs.create(outputPath, true); // Create/overwrite file in HDFS
        // Wrap the HDFS output stream to write characters efficiently
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        writer.write(jsonArray.toString(2)); // Write the JSON array to the file (with 2-space indentation for readability)
        writer.close(); // Close the writer and the underlying stream
    }

    /**
     * Writes the patient data classified by Region to a JSON array on HDFS.
     * Output format: [{"region": "...", "patients": [...][{"symptoms": "...", "gender": "...", ...}]}, ...]
     * @param regionMap Map of Region to a list of Patients.
     * @param outputPath The HDFS Path where the JSON file will be written.
     * @param fs The Hadoop FileSystem object.
     * @throws IOException If an I/O error occurs.
     */
    private static void writeRegionOutputToJson(Map<String, List<Patient>> regionMap, Path outputPath, FileSystem fs) throws IOException {
        JSONArray jsonArray = new JSONArray(); // Create a new JSON array

        // Iterate over the entries in the regionMap (Region -> List of Patients)
        for (Map.Entry<String, List<Patient>> entry : regionMap.entrySet()) {
            JSONObject jsonObject = new JSONObject(); // Create a JSON object for each region group
            jsonObject.put("region", entry.getKey()); // Put the region name as a field

            JSONArray patientsArray = new JSONArray(); // Create a JSON array to hold patients in this region
            // Iterate over the list of patients in the current region
            for (Patient patient : entry.getValue()) {
                patientsArray.put(patientToJson(patient)); // Add each Patient object (converted to JSON) to the patients array
            }
            jsonObject.put("patients", patientsArray); // Put the patients array into the region object
            jsonArray.put(jsonObject); // Add the region object to the main JSON array
        }

        // Write the JSON array to the specified HDFS file path
        FSDataOutputStream outputStream = fs.create(outputPath, true); // Create/overwrite file in HDFS
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        writer.write(jsonArray.toString(2)); // Write the JSON array (pretty printed)
        writer.close(); // Close resources
    }

    /**
     * Writes the patient data classified by Normalized Symptom to a JSON array on HDFS.
     * Includes a group ID and a human-readable symptom label.
     * Output format: [{"symptomGroupId": ..., "symptomLabel": "...", "patients": [...][{"symptoms": "...", ...}]}, ...]
     * @param symptomMap Map of Normalized Symptom to a list of Patients.
     * @param normalizedToOriginalSymptom Map to get a human-readable label for each normalized symptom group.
     * @param outputPath The HDFS Path where the JSON file will be written.
     * @param fs The Hadoop FileSystem object.
     * @throws IOException If an I/O error occurs.
     */
    private static void writeSymptomOutputToJson(Map<String, List<Patient>> symptomMap, Map<String, String> normalizedToOriginalSymptom, Path outputPath, FileSystem fs) throws IOException {
        JSONArray jsonArray = new JSONArray(); // Create a new JSON array
        int groupId = 1; // Counter for assigning unique symptom group IDs

        // Iterate over the entries in the symptomMap (Normalized Symptom -> List of Patients)
        for (Map.Entry<String, List<Patient>> entry : symptomMap.entrySet()) {
            JSONObject jsonObject = new JSONObject(); // Create a JSON object for each symptom group
            jsonObject.put("symptomGroupId", groupId++); // Assign a unique ID to the group
            // Use the stored original symptom as a human-readable label for the group
            jsonObject.put("symptomLabel", normalizedToOriginalSymptom.get(entry.getKey()));

            JSONArray patientsArray = new JSONArray(); // Create a JSON array to hold patients in this symptom group
            // Iterate over the list of patients in the current symptom group
            for (Patient patient : entry.getValue()) {
                patientsArray.put(patientToJson(patient)); // Add each Patient object (converted to JSON) to the patients array
            }
            jsonObject.put("patients", patientsArray); // Put the patients array into the symptom group object
            jsonArray.put(jsonObject); // Add the symptom group object to the main JSON array
        }

        // Write the JSON array to the specified HDFS file path
        FSDataOutputStream outputStream = fs.create(outputPath, true); // Create/overwrite file in HDFS
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        writer.write(jsonArray.toString(2)); // Write the JSON array (pretty printed)
        writer.close(); // Close resources
    }


    /**
     * Helper method to convert a Patient object into a JSONObject.
     * @param patient The Patient object to convert.
     * @return A JSONObject representing the Patient.
     */
    private static JSONObject patientToJson(Patient patient) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("patientId", patient.getPatientId());
        jsonObject.put("name", patient.getName());
        jsonObject.put("age", patient.getAge());
        jsonObject.put("gender", patient.getGender());
        jsonObject.put("region", patient.getRegion());
        jsonObject.put("symptoms", patient.getSymptoms()); // Include the original symptom string
        // Add other patient attributes here if needed
        return jsonObject;
    }
}