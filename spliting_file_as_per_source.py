import os
import pandas as pd

class DataFrameSplitter:
    def __init__(self, input_folder, output_folder):
        self.input_folder = input_folder
        self.output_folder = output_folder

    def split_dataframe(self, df, chunk_size):
        chunks = [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
        return chunks

    def process_files(self):
        for file in os.listdir(self.input_folder):
            if file.endswith('.xlsx'):
                file_name = file.split(".")[0].split("_")
                city = file_name[2]
                new_output_path = os.path.join(self.output_folder, city)

                if not os.path.exists(new_output_path):
                    os.makedirs(new_output_path)

                df = pd.read_excel(os.path.join(self.input_folder, file))
                
                if 'person_city_for_MO' in df.columns:
                    df.rename(columns={'person_city_for_MO': 'person_city'}, inplace=True)

                self.process_levels(df, city, new_output_path)

    def process_levels(self, df, city, output_path):
        for level in [1, 2]:
            df_filtered = df[df['Level'] == level]
            chunks = self.split_dataframe(df_filtered, 200)
            self.save_chunks(chunks, city, level, output_path)

    def save_chunks(self, chunks, city, level, output_path):
        for i in range(min(5, len(chunks))):
            if level == 1:
                output_file = os.path.join(output_path, f'{city}_HNI_Level_{level}_batch_{5+i}.xlsx')
            elif level == 2:
                output_file = os.path.join(output_path, f'{city}_HNI_Level_{level}_batch_{10+i}.xlsx')

            chunks[i].to_excel(output_file, index=False)
            print(f'Saved: {output_file}')

# Example usage
input_folder = "C:\\Users\\amola\\OneDrive\\Documents\\pankaj\\inhouse_email_marketing\\delivery_data_cities\\"
output_folder = 'C:\\Users\\amola\\OneDrive\\Documents\\pankaj\\inhouse_email_marketing\\delivery_data_cities\\output_folder\\'

splitter = DataFrameSplitter(input_folder, output_folder)
splitter.process_files()
