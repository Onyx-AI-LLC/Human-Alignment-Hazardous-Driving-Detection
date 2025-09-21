import axios from "axios";
import { ReferralCode } from "../utils/interfaces";

const useValidateReferral = () => {
    const validateReferral = async (referralCode: ReferralCode) => {
        try {
            const response = await axios.post('https://human-alignment-hazardous-driving.onrender.com/auth/validateReferral', referralCode);

            if (response.data.isValid) {
                return { isValid: true }
            } else {
                return { isValid: false }
            }

        } catch (error) {
            throw new Error(`An error has occurred while signing in: ${error}`);
        }
    };

    return { validateReferral }
}

export default useValidateReferral;