import sys
import pandas as pd


def process_data(input_csv, output_csv):
    try:
        
        df = pd.read_csv(input_csv)

        
        df['New_Price'] = df['Open'].apply(lambda x: x/2)

        
        df.to_csv(output_csv, index=False)
        print(f"Data Transformation is Done")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Give Another File")
    else:
        input_csv = sys.argv[1]
        output_csv = sys.argv[2]
        process_data(input_csv, output_csv)
