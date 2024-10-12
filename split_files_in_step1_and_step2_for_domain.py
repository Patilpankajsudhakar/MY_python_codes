import pandas as pd
import os
import math

#step 1 filter the data from source column and create spliting file and store it
main_input_folder = "C:\\Users\\amola\\OneDrive\\Documents\\pankaj\\inhouse_email_marketing\\spliting_55k_data\\55k_hot_audiences_ready_campaign_data.xlsx"
main_output_folder = 'C:\\Users\\amola\\OneDrive\\Documents\\pankaj\\inhouse_email_marketing\\spliting_55k_data\\split_source_output_folder\\'

if not os.path.exists(main_output_folder):
    os.makedirs(main_output_folder)

df = pd.read_excel(main_input_folder,sheet_name='Sheet1')
df.head()
source_list=df['source'].unique()
print(len(source_list))

#step to split source column
for source_name in source_list:
    source_name1=source_name.split(".")
    file_name=source_name1[0]
    df_filtered=df[df['source'] == source_name]
    output_file = os.path.join(main_output_folder, f'{file_name}_.xlsx')
    df_filtered.to_excel(output_file, index=False)
    print(f'Saved: {output_file}')


#step2 filter the data from domain column and create spliting file into 200 chunk for the domainwise and store it
step2_input_folder = "C:\\Users\\amola\\OneDrive\\Documents\\pankaj\\inhouse_email_marketing\\spliting_55k_data\\split_source_output_folder\\"
step2_output_folder = 'C:\\Users\\amola\\OneDrive\\Documents\\pankaj\\inhouse_email_marketing\\spliting_55k_data\\domain_output_folder\\'


def split_dataframe(df, chunk_size):
    chunks = [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
    return chunks

for file in os.listdir(step2_input_folder):
    if  file.endswith('.xlsx'):
        file_name=file.split(".")
        file_name=file_name[0].replace(" ","_")
        new_output_path=step2_output_folder + "\\" + file_name
        if not os.path.exists(new_output_path):
            os.makedirs(new_output_path)
        
        df_domain = pd.read_excel(step2_input_folder+file)
        domain_list=df_domain['domain'].unique()
        combined_df_domain_Lt200 = pd.DataFrame()
        combined_df_chunks_Lt200 = pd.DataFrame()
        batch_size = 200
        batches = []
        current_batch = pd.DataFrame()
        processed_domains = set()  # Track domains already processed
        batch_idx = 1  # Batch index
        rows_in_batch = 0  # Number of rows in the current batch
        contact_chunk_domains=""
        domain_less_200_pth=new_output_path + "\\" + "other_files_split"
        if not os.path.exists(domain_less_200_pth):
            os.makedirs(domain_less_200_pth)
        try:
            for domain_name in domain_list:
                domain_name1=domain_name.replace(".","_")
                df_domain_filtered=df_domain[df_domain['domain'] == domain_name]
                if len(df_domain_filtered)>=200:
                    chunks=split_dataframe(df_domain_filtered, 200)
                    for i, chunk in enumerate(chunks):
                        try:
                            if len(chunk) >= 200:
                                output_file_chunk = os.path.join(new_output_path, f'{domain_name1}_batch_{i}.xlsx')
                                chunk.to_excel(output_file_chunk, index=False)
                                print(f'Saved: files that domain size is greater than 200 {output_file_chunk}')
                            else:
                                combined_df_chunks_Lt200=pd.concat([combined_df_chunks_Lt200, chunk], ignore_index=True)
                                contact_chunk_domains=contact_chunk_domains+domain_name
                        except Exception as a:
                            print(a, f'error occur {output_file_chunk}')
                else:
                    combined_df_domain_Lt200=pd.concat([combined_df_domain_Lt200, df_domain_filtered], ignore_index=True)
            comb_output_file_chunk = os.path.join(new_output_path, f'{contact_chunk_domains}.xlsx')
            combined_df_chunks_Lt200.to_excel(comb_output_file_chunk, index=False)
            print(f'Saved: other files in the chunk {comb_output_file_chunk}')
        except Exception as b:
            print(b,"error having in the domain section")


        domain_groups = {domain: df for domain, df in combined_df_domain_Lt200.groupby('domain')}
        for domain, domain_data in domain_groups.items():
            if domain in processed_domains:
                continue  # Skip if domain has already been added to a batch

            while not domain_data.empty:
                # Calculate how many rows to take from this domain
                rows_to_add = min(len(domain_data), batch_size - rows_in_batch)
                # Append rows to the current batch
                current_batch = pd.concat([current_batch, domain_data.iloc[:rows_to_add]], ignore_index=True)
                rows_in_batch += rows_to_add
                # Drop the rows we just added from domain_data
                domain_data = domain_data.iloc[rows_to_add:]
                # If we reach the batch size, save and start a new batch
                try:
                    if rows_in_batch >= batch_size:
                        # Save current badomaintch
                        output_file_less200comb = os.path.join(domain_less_200_pth, f'batch_{batch_idx}.xlsx')
                        current_batch.to_excel(output_file_less200comb, index=False)
                        print(f'Saved: file who domain is less than 200 {file_name}+batch_{batch_idx}.xlx')
                        batch_idx += 1
                        # Reset for the next batch
                        current_batch = pd.DataFrame()
                        rows_in_batch = 0
                except Exception as c:
                    print(c, "error in batch size section batch less than 200")
                # Mark this domain as processed
            processed_domains.add(domain)
            # If there's any remaining data in the current batch, save it
            try:
                if not current_batch.empty:
                    output_file_less200comb = os.path.join(domain_less_200_pth, f'batch_{batch_idx}.xlsx')
                    current_batch.to_excel(output_file_less200comb, index=False)
                    print(f'Saved: remain in domain who less than 200 {file_name}+batch_{batch_idx}.xlsx')
            except Exception as d:
                print(d, "error in remaining current batch section")

            #output_file_comb = os.path.join(new_output_path, f'finalremain_contact_domains.xlsx')
            #combined_df_domain_Lt200.to_excel(output_file_comb, index=False)

