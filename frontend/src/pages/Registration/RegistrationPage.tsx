import styles from './RegistrationPage.module.css';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form'
import Row from 'react-bootstrap/Row';
import { RegistrationFormData } from '../../utils/types';
import { ReferralCode } from '../../utils/interfaces';
import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import useRegister from '../../hooks/useRegister';
import useValidateReferral from '../../hooks/useValidateReferral';
import { Container } from 'react-bootstrap';
import { countries } from '../../utils/countries';
import HahdHeader from '../../components/hahd/HahdHeader';

const RegistrationPage: React.FC = () => {
    const navigate = useNavigate();
    const { registerUser } = useRegister();
    const { validateReferral } = useValidateReferral();

    const [isValidReferral, setIsValidReferral] = useState<boolean>();
    const [isScreenTooSmall, setIsScreenTooSmall] = useState(false);

    const REFERRAL_CODE_LENGTH = 36;

    const [formData, setFormData] = useState<RegistrationFormData>({
        email: '',
        password: '',
        referredByUser: '',
        country: '',
        state: '',
        city: '',
        licenseAge: '',
        age: 0,
        ethnicity: '',
        gender: '',
        visuallyImpaired: false
    });

    const [confirmedPassword, setConfirmedPassword] = useState('');

    const states = [
        { code: 'AL', name: 'Alabama' },
        { code: 'AK', name: 'Alaska' },
        { code: 'AZ', name: 'Arizona' },
        { code: 'AR', name: 'Arkansas' },
        { code: 'CA', name: 'California' },
        { code: 'CO', name: 'Colorado' },
        { code: 'CT', name: 'Connecticut' },
        { code: 'DE', name: 'Delaware' },
        { code: 'FL', name: 'Florida' },
        { code: 'GA', name: 'Georgia' },
        { code: 'HI', name: 'Hawaii' },
        { code: 'ID', name: 'Idaho' },
        { code: 'IL', name: 'Illinois' },
        { code: 'IN', name: 'Indiana' },
        { code: 'IA', name: 'Iowa' },
        { code: 'KS', name: 'Kansas' },
        { code: 'KY', name: 'Kentucky' },
        { code: 'LA', name: 'Louisiana' },
        { code: 'ME', name: 'Maine' },
        { code: 'MD', name: 'Maryland' },
        { code: 'MA', name: 'Massachusetts' },
        { code: 'MI', name: 'Michigan' },
        { code: 'MN', name: 'Minnesota' },
        { code: 'MS', name: 'Mississippi' },
        { code: 'MO', name: 'Missouri' },
        { code: 'MT', name: 'Montana' },
        { code: 'NE', name: 'Nebraska' },
        { code: 'NV', name: 'Nevada' },
        { code: 'NH', name: 'New Hampshire' },
        { code: 'NJ', name: 'New Jersey' },
        { code: 'NM', name: 'New Mexico' },
        { code: 'NY', name: 'New York' },
        { code: 'NC', name: 'North Carolina' },
        { code: 'ND', name: 'North Dakota' },
        { code: 'OH', name: 'Ohio' },
        { code: 'OK', name: 'Oklahoma' },
        { code: 'OR', name: 'Oregon' },
        { code: 'PA', name: 'Pennsylvania' },
        { code: 'RI', name: 'Rhode Island' },
        { code: 'SC', name: 'South Carolina' },
        { code: 'SD', name: 'South Dakota' },
        { code: 'TN', name: 'Tennessee' },
        { code: 'TX', name: 'Texas' },
        { code: 'UT', name: 'Utah' },
        { code: 'VT', name: 'Vermont' },
        { code: 'VA', name: 'Virginia' },
        { code: 'WA', name: 'Washington' },
        { code: 'WV', name: 'West Virginia' },
        { code: 'WI', name: 'Wisconsin' },
        { code: 'WY', name: 'Wyoming' }
    ];

    const ethnicities = [
        "Asian",
        "Black or African American",
        "Hispanic or Latino",
        "Native American or Alaska Native",
        "Native Hawaiian or Other Pacific Islander",
        "White",
        "Middle Eastern or North African",
        "Multiracial",
        "Other",
        "Prefer not to say",
    ];

    const handleNavigate = () => {
        navigate('/')
    };

    const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault();

        const result = await registerUser(formData);
        if (result.success) {
            navigate('/');
        } else {

            console.log('An error has occurred during registration.')
        }
    };

    const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
        const { name, value, type } = e.target;

        let updatedValue: any;

        if (type === 'checkbox') {
            updatedValue = (e.target as HTMLInputElement).checked;
        } else {
            updatedValue = value;
        }

        setFormData((prevState) => ({
            ...prevState,
            [name]: updatedValue,
        }));

        if (name === 'referredByUser' && value.length == REFERRAL_CODE_LENGTH) {
            const referralCode: ReferralCode = { code: value }
            handleValidateReferral(referralCode);
        } else if (name === 'referredByUser' && value.length >= 1) {
            setIsValidReferral(false);
        }
    };

    const handleValidateReferral = async (referralCode: ReferralCode) => {
        const response = await validateReferral(referralCode)
        if (response.isValid) {
            setIsValidReferral(true);
        } else {
            setIsValidReferral(false);
        }
    }

    const handleConfirmPassword = (e: React.ChangeEvent<HTMLInputElement>) => {
        const password = e.target.value;
        setConfirmedPassword(password)
    };

    useEffect(() => {
        const urlParams = new URLSearchParams(window.location.search);
        const referralParam = urlParams.get('referral');

        if (referralParam && referralParam.length === REFERRAL_CODE_LENGTH) {
            setFormData(prev => ({
                ...prev,
                referredByUser: referralParam
            }));

            handleValidateReferral({ code: referralParam });
        }
    }, []);

    useEffect(() => {
        const checkScreenSize = () => {
            setIsScreenTooSmall(window.innerWidth < 1062);
        };

        checkScreenSize();
        window.addEventListener('resize', checkScreenSize);
        return () => window.removeEventListener('resize', checkScreenSize);
    }, []);

    useEffect(() => {
        // Enable scrolling for registration page
        document.body.style.overflow = 'auto';
        document.documentElement.style.overflow = 'auto';
        const root = document.getElementById('root');
        if (root) {
            root.style.overflow = 'auto';
            root.style.height = 'auto';
        }

        return () => {
            // Reset on cleanup
            document.body.style.overflow = '';
            document.documentElement.style.overflow = '';
            if (root) {
                root.style.overflow = '';
                root.style.height = '100vh';
            }
        };
    }, []);

    if (isScreenTooSmall) {
        return (
            <div className={styles.screenWarning}>
                <i className={`bi bi-emoji-frown-fill ${styles.floatingIcon}`}></i>
                <h2>Screen Size Too Small</h2>
                <p>
                    For the best experience with our<span style={{ fontWeight: '700', color: '#FFD700' }}> Human-Aligned Hazard Detection Survey</span>,
                    please increase your window size or switch to a larger device.
                    Your feedback is important to us!
                </p>
            </div>
        );
    }

    return (
        <>
            <HahdHeader />
            <div className={`${styles.container} registration-page`}>
            <Container className={styles.containerWrapper}>
                <Row>
                    {/* First Column */}
                    <Col
                        md={12}
                        className="d-flex flex-column justify-content-center align-items-center px-5">
                        <div style={{ maxWidth: '650px' }}>
                            <h1 className={`${styles.title} mb-4 text-center`}>
                                Survey Registration
                            </h1>
                            <p className={`${styles.content} mb-2`}>
                                <span style={{ color: 'red' }}>*</span> This form collects information to understand how different backgrounds influence hazard detection while driving. Your responses are confidential and used only for research purposes.
                            </p>
                            <p className={`${styles.content} mb-4`}>
                                <span style={{ color: 'red' }}>*</span> Participation is voluntary
                            </p>
                        </div>
                        <div className='mb-4' style={{ width: '700px', border: '0.2px solid grey' }}></div>
                        <div style={{ maxWidth: '700px', width: '100%' }}>
                            <Form onSubmit={handleSubmit} style={{ color: '#4a5568' }}>
                                <Form.Group className='mb-4' controlId="formGridEmail" >
                                    <Form.Label>Email</Form.Label>
                                    <Form.Control
                                        type="email"
                                        placeholder="name@example.com"
                                        name="email"
                                        value={formData.email}
                                        onChange={handleChange}
                                        required
                                    />
                                </Form.Group>

                                <Row className='mb-4'>
                                    <Form.Group as={Col} controlId="formGridPassword">
                                        <Form.Label>Password</Form.Label>
                                        <Form.Control
                                            type="password"
                                            placeholder="Enter Password"
                                            name="password"
                                            value={formData.password}
                                            onChange={handleChange}
                                            required
                                        />
                                    </Form.Group>

                                    <Form.Group as={Col} controlId="formGridPassword">
                                        <Form.Label>Confirm Password</Form.Label>
                                        <Form.Control
                                            type="password"
                                            placeholder="Confirm Password"
                                            name="confirmedPassword"
                                            value={confirmedPassword}
                                            onChange={handleConfirmPassword}
                                            required
                                            disabled={formData.password ? false : true}
                                            isInvalid={confirmedPassword && formData.password && confirmedPassword != formData.password ? true : false}
                                        />
                                        <Form.Control.Feedback type="invalid">
                                            Passwords do not match.
                                        </Form.Control.Feedback>
                                    </Form.Group>
                                </Row>

                                <Form.Group className='mb-4' controlId="formGridCountry" >
                                    <Form.Label>Country</Form.Label>
                                    <Form.Control
                                        as="select"
                                        name="country"
                                        placeholder="name@example.com"
                                        value={formData.country}
                                        onChange={handleChange}
                                        required
                                    >
                                        <option value="">Select...</option>
                                        {countries.map((country) => (
                                            <option key={country.code} value={country.code}>
                                                {country.name}
                                            </option>
                                        ))}
                                    </Form.Control>
                                </Form.Group>

                                {formData.country === 'US' && (
                                    <Row className="mb-4">
                                        <Form.Group as={Col} controlId="formGridCity">
                                            <Form.Label>City</Form.Label>
                                            <Form.Control
                                                type="text"
                                                placeholder="Enter City"
                                                name="city"
                                                value={formData.city || ""}
                                                onChange={handleChange}
                                                required
                                            />
                                        </Form.Group>

                                        <Form.Group as={Col} controlId="formGridState">
                                            <Form.Label>State</Form.Label>
                                            <Form.Control
                                                as="select"
                                                name="state"
                                                value={formData.state || ""}
                                                onChange={handleChange}
                                                required
                                            >
                                                <option value="">Select...</option>
                                                {states.map((state, index) => (
                                                    <option key={index} value={state.name}>
                                                        {state.name}
                                                    </option>
                                                ))}
                                            </Form.Control>
                                        </Form.Group>
                                    </Row>
                                )}

                                <Row className="mb-4">
                                    {/* Current Age */}
                                    <Form.Group as={Col} controlId="formAge">
                                        <Form.Label>Age</Form.Label>
                                        <Form.Control
                                            type="number"
                                            placeholder="Enter age"
                                            name="age"
                                            min="0"
                                            max="100"
                                            required
                                            value={formData.age || ''}
                                            onChange={(e) => {
                                                const inputValue = e.target.value;
                                                if (inputValue === '') {
                                                    setFormData({ ...formData, age: 0 });
                                                } else {
                                                    const value = Math.max(0, Math.min(150, Number(inputValue)));
                                                    setFormData({ ...formData, age: value });
                                                }
                                            }}
                                        />
                                    </Form.Group>

                                    {/* Gender */}
                                    <Form.Group as={Col} controlId="formGender">
                                        <Form.Label>Gender</Form.Label>
                                        <Form.Select name="gender" value={formData.gender} onChange={handleChange}>
                                            <option value="">Select...</option>
                                            <option value="male">Male</option>
                                            <option value="female">Female</option>
                                            <option value="prefer-not-to-say">Prefer not to say</option>
                                        </Form.Select>
                                    </Form.Group>
                                </Row>

                                {/* Ethnicity */}
                                <Form.Group className="mb-4" controlId="formEthnicity">
                                    <Form.Label>Ethnicity</Form.Label>
                                    <Form.Control
                                        as="select"
                                        name="ethnicity"
                                        value={formData.ethnicity}
                                        onChange={handleChange}
                                    >
                                        <option value="">Select...</option>
                                        {ethnicities.map((ethnicity, index) => (
                                            <option key={index} value={ethnicity}>
                                                {ethnicity}
                                            </option>
                                        ))}
                                    </Form.Control>
                                </Form.Group>

                                {/* License Age */}
                                <Form.Group className="mb-4" controlId="formLicenseAge">
                                    <Form.Label>At what age did you obtain your drivers license (approximately)?</Form.Label>
                                    <Form.Control
                                        type="number"
                                        placeholder="Enter age"
                                        name="licenseAge"
                                        value={formData.licenseAge}
                                        onChange={handleChange}
                                        required
                                    />
                                </Form.Group>

                                {/* Visually Impaired */}
                                <Form.Group className="mb-4" controlId="formVisuallyImpaired">
                                    <Form.Label>Are you visually impaired or do you wear glasses?</Form.Label>
                                    <div>
                                        <Form.Check
                                            type="radio"
                                            label="Yes"
                                            name="visuallyImpaired"
                                            value="true"
                                            checked={formData.visuallyImpaired === true}
                                            onChange={(e) => {
                                                const value = e.target.value === "true";
                                                setFormData(prevState => ({
                                                    ...prevState,
                                                    visuallyImpaired: value
                                                }));
                                            }}
                                        />
                                        <Form.Check
                                            type="radio"
                                            label="No"
                                            name="visuallyImpaired"
                                            value="false"
                                            checked={formData.visuallyImpaired === false}
                                            onChange={(e) => {
                                                const value = e.target.value === "true";
                                                setFormData(prevState => ({
                                                    ...prevState,
                                                    visuallyImpaired: value
                                                }));
                                            }}
                                        />
                                    </div>
                                </Form.Group>

                                {/* Enter Referral Code */}
                                <Form.Group className='mb-4 position-relative' controlId="formGridReferredByUser">
                                    <Form.Label>Referral Code (Optional)</Form.Label>
                                    <div className="d-flex align-items-center">
                                        <Form.Control
                                            type="text"
                                            placeholder="xxxxx-xxxxx-xxxxx-xxxxx"
                                            name="referredByUser"
                                            value={formData.referredByUser}
                                            onChange={handleChange}
                                            className={formData.referredByUser ? (isValidReferral ? 'is-valid' : 'is-invalid') : ''}
                                        />
                                    </div>
                                </Form.Group>

                                <div className={styles.buttonGroup}>
                                    <Button variant="dark" type="submit" style={{ padding: '6px 45px 6px 45px', marginTop: '25px', backgroundColor: '#2d3748' }}>
                                        Submit
                                    </Button>
                                    <Button variant="danger" onClick={handleNavigate} style={{ padding: '6px 45px 6px 45px', marginTop: '25px' }}>
                                        Cancel
                                    </Button>
                                </div>
                            </Form>
                        </div>
                    </Col>
                </Row>
            </Container>
        </div>
        </>
    )
}

export default RegistrationPage;