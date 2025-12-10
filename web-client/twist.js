/**
 * Twist Protocol Module
 * =====================
 * 
 * Binary encoding/decoding for ROS2 geometry_msgs/Twist messages.
 * 
 * Binary Format (48 bytes, little-endian IEEE 754 double-precision):
 *   Offset  Size    Type      Field
 *   ─────────────────────────────────
 *   0       8       float64   linear.x
 *   8       8       float64   linear.y
 *   16      8       float64   linear.z
 *   24      8       float64   angular.x
 *   32      8       float64   angular.y
 *   40      8       float64   angular.z
 *   ─────────────────────────────────
 *   Total: 48 bytes
 * 
 * @module TwistProtocol
 */

// Message size constant
const TWIST_SIZE = 48;

/**
 * TwistMessage class representing ROS2 geometry_msgs/Twist
 */
class TwistMessage {
    /**
     * Create a TwistMessage
     * @param {Object} options - Velocity components
     * @param {number} [options.linearX=0] - Linear X velocity (m/s)
     * @param {number} [options.linearY=0] - Linear Y velocity (m/s)
     * @param {number} [options.linearZ=0] - Linear Z velocity (m/s)
     * @param {number} [options.angularX=0] - Angular X velocity (rad/s)
     * @param {number} [options.angularY=0] - Angular Y velocity (rad/s)
     * @param {number} [options.angularZ=0] - Angular Z velocity (rad/s)
     */
    constructor({
        linearX = 0,
        linearY = 0,
        linearZ = 0,
        angularX = 0,
        angularY = 0,
        angularZ = 0
    } = {}) {
        this.linear = {
            x: linearX,
            y: linearY,
            z: linearZ
        };
        this.angular = {
            x: angularX,
            y: angularY,
            z: angularZ
        };
    }

    /**
     * Encode the Twist message to binary format
     * @returns {ArrayBuffer} 48-byte binary representation
     */
    encode() {
        const buffer = new ArrayBuffer(TWIST_SIZE);
        const view = new DataView(buffer);
        
        // Little-endian encoding
        view.setFloat64(0, this.linear.x, true);
        view.setFloat64(8, this.linear.y, true);
        view.setFloat64(16, this.linear.z, true);
        view.setFloat64(24, this.angular.x, true);
        view.setFloat64(32, this.angular.y, true);
        view.setFloat64(40, this.angular.z, true);
        
        return buffer;
    }

    /**
     * Decode binary data into a TwistMessage
     * @param {ArrayBuffer} buffer - 48-byte binary data
     * @returns {TwistMessage} Decoded message
     * @throws {Error} If buffer size is invalid
     */
    static decode(buffer) {
        if (buffer.byteLength !== TWIST_SIZE) {
            throw new Error(
                `Invalid twist message size: expected ${TWIST_SIZE} bytes, ` +
                `got ${buffer.byteLength} bytes`
            );
        }
        
        const view = new DataView(buffer);
        
        return new TwistMessage({
            linearX: view.getFloat64(0, true),
            linearY: view.getFloat64(8, true),
            linearZ: view.getFloat64(16, true),
            angularX: view.getFloat64(24, true),
            angularY: view.getFloat64(32, true),
            angularZ: view.getFloat64(40, true)
        });
    }

    /**
     * Create a zero velocity message (stop command)
     * @returns {TwistMessage}
     */
    static zero() {
        return new TwistMessage();
    }

    /**
     * Create a forward motion command
     * @param {number} [speed=1.0] - Forward speed in m/s
     * @returns {TwistMessage}
     */
    static forward(speed = 1.0) {
        return new TwistMessage({ linearY: Math.abs(speed) });
    }

    /**
     * Create a backward motion command
     * @param {number} [speed=1.0] - Backward speed in m/s
     * @returns {TwistMessage}
     */
    static backward(speed = 1.0) {
        return new TwistMessage({ linearY: -Math.abs(speed) });
    }

    /**
     * Create a left turn command
     * @param {number} [rate=1.0] - Turn rate in rad/s
     * @returns {TwistMessage}
     */
    static turnLeft(rate = 1.0) {
        return new TwistMessage({ angularZ: Math.abs(rate) });
    }

    /**
     * Create a right turn command
     * @param {number} [rate=1.0] - Turn rate in rad/s
     * @returns {TwistMessage}
     */
    static turnRight(rate = 1.0) {
        return new TwistMessage({ angularZ: -Math.abs(rate) });
    }

    /**
     * Check if this is a stop command (all zeros)
     * @returns {boolean}
     */
    isZero() {
        return this.linear.x === 0 && this.linear.y === 0 && this.linear.z === 0 &&
               this.angular.x === 0 && this.angular.y === 0 && this.angular.z === 0;
    }

    /**
     * Get string representation
     * @returns {string}
     */
    toString() {
        const lin = this.linear;
        const ang = this.angular;
        return `Twist{linear: [${lin.x.toFixed(3)}, ${lin.y.toFixed(3)}, ${lin.z.toFixed(3)}], ` +
               `angular: [${ang.x.toFixed(3)}, ${ang.y.toFixed(3)}, ${ang.z.toFixed(3)}]}`;
    }

    /**
     * Clone the message
     * @returns {TwistMessage}
     */
    clone() {
        return new TwistMessage({
            linearX: this.linear.x,
            linearY: this.linear.y,
            linearZ: this.linear.z,
            angularX: this.angular.x,
            angularY: this.angular.y,
            angularZ: this.angular.z
        });
    }
}

/**
 * Validate that data is a valid Twist message
 * @param {ArrayBuffer} buffer - Data to validate
 * @returns {boolean}
 */
function validateTwistData(buffer) {
    return buffer && buffer.byteLength === TWIST_SIZE;
}

/**
 * Quick encode function for direct values
 * @param {number} linearY - Linear Y velocity
 * @param {number} angularZ - Angular Z velocity
 * @returns {ArrayBuffer}
 */
function encodeTwist(linearY = 0, angularZ = 0) {
    return new TwistMessage({ linearY, angularZ }).encode();
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        TwistMessage,
        TWIST_SIZE,
        validateTwistData,
        encodeTwist
    };
}