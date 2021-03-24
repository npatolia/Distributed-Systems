public class Record implements Comparable <Record>{
    private int id;
    private String productName;
    private double price;
    
    public Record(int id, String productName, double price){
        this.id = id;
        this.productName = productName;
        this.price = price;
    }
    public String toString(){
        return id+","+productName+","+price;
    }
    public int compareTo(Record other){ //descending order
        if(this.price > other.price) {
            return -1;
        } 
        if(this.price < other.price){
            return 1;
        }
        return 0;
    }
}
