import 'dotenv/config'
import { getEnvironment } from './environment'

describe('Environment variable', () => {
  it('Should create, configure, and load without exception thrown', () => {
    // create env config before running test
    expect(() => getEnvironment()).not.toThrow()
  })
})
