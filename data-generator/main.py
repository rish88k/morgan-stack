import schema
import seed_data

def main():
    schema.create_schema()
    seed_data.generate_data()

if __name__ == "__main__":
    main()
