package src; // Make sure this package declaration is at the top

/**
 * Represents a Patient with various attributes.
 */
public class Patient {
    // Private member variables to store patient data
    private String patientId;
    private String name;
    private int age;
    private String gender;
    private String region;
    private String symptoms; // Stores the original symptom string from the CSV

    // Constructor (optional, but good practice - not strictly needed for this code's creation method)
    public Patient() {
        // Default constructor
    }

    // --- Getter Methods ---
    // Provide access to the private member variables

    public String getPatientId() {
        return patientId;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getGender() {
        return gender;
    }

    public String getRegion() {
        return region;
    }

    public String getSymptoms() {
        return symptoms;
    }

    // --- Setter Methods ---
    // Allow setting the values of the private member variables

    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setSymptoms(String symptoms) {
        this.symptoms = symptoms;
    }

    // --- toString Method ---
    // Provides a string representation of a Patient object (useful for debugging)
    @Override
    public String toString() {
        return "Patient{" +
                "patientId='" + patientId + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", gender='" + gender + '\'' +
                ", region='" + region + '\'' +
                ", symptoms='" + symptoms + '\'' +
                '}';
    }
}