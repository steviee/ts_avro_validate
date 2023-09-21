import avro from 'avsc';

// ====[ CONFIG ]======================
const SCHEMA_ID = 2
const SCHEMA_REGISTRY_URL= "http://localhost:8081/schemas/ids"
// ====================================


// Here we have some schema
const defaultSchema = avro.Type.forSchema({
    type: 'record',
    name: 'HelloWorldMessage',      // the schema ID
    fields: [
        { name: 'message', type: 'string' },
        { name: 'sender', type: 'string' }
    ]
});

async function getSchema(id: number):Promise<string> {

    type GetSchemaResponse  = {
        schema: string;
    };

    try {
      // 👇️ const response: Response
      const response = await fetch(`${SCHEMA_REGISTRY_URL}/${id}`, {
        method: 'GET',
        headers: {
          Accept: 'application/json',
        },
      });
  
      if (!response.ok) {
        throw new Error(`Error! status: ${response.status}`);
      }
  
      const result = (await response.json()) as GetSchemaResponse;

      return result.schema as string;

    } catch (error) {
      if (error instanceof Error) {
        console.log('error message: ', error.message);
        return error.message;
      } else {
        console.log('unexpected error: ', error);
        return 'An unexpected error occurred';
      }
    }
  }

// Very simple validation of some JSON thingy using the hard-coded schema
export function Validate(input: string): boolean {
    const obj = JSON.parse(input);
    return defaultSchema.isValid(obj);
}

// Very simple validation of some JSON thingy using a schema registry
// Please note that this call will always call the schema registry and should probably be
// enhanced with some form of caching.
export async function ValidateWithRegistry(input: string, schemaId: number): Promise<boolean> {

    const schemaString = await getSchema(schemaId);
    const remoteSchema = avro.Type.forSchema(JSON.parse(schemaString));

    const obj = JSON.parse(input);
    return remoteSchema.isValid(obj);
}


// Combine everything and test with the hardcoded schema
function main() {

    const validString = JSON.stringify({ message: 'be home early!', sender: 'momma' });
    const invalidString = JSON.stringify({ message: 'no beer today!', from: 'momma', });

    console.info("Validating schema with local (hardcoded) schema:");
    console.info(`First check: ${Validate(validString)} `)
    console.info(`Second check: ${Validate(invalidString)} `)

}

// Perform the same test, but with a real schema registry
async function asyncMain() {

    const validString = JSON.stringify({ message: 'be home early!', sender: 'momma' });
    const invalidString = JSON.stringify({ message: 'no beer today!', from: 'momma', });

    const valid: boolean = await ValidateWithRegistry(validString, SCHEMA_ID);
    const invalid: boolean = await ValidateWithRegistry(invalidString, SCHEMA_ID);

    console.info("Validating schema with remote schema:");
    console.info(`First remote check: ${valid} `)
    console.info(`Second remote check: ${invalid} `)

}

// run
main(); // one example straight forward with locally hardcoded schema
asyncMain(); // and one example with async/await and remote registry