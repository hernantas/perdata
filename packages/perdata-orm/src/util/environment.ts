import 'dotenv/config'
import { TypeOf, literal, number, object, string } from 'pertype'

const Environment = object({
  CLIENT: literal('pg'),
  HOST: string(),
  PORT: number(),
  USER: string(),
  PASSWORD: string(),
  DATABASE: string(),
})

export type Environment = TypeOf<typeof Environment>

/**
 * Get environment variable
 * @returns Environment variable
 */
export function getEnvironment(): Environment {
  return Environment.decode(process.env)
}
