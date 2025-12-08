package bftsmart.demo.smallbank;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Message format for SmallBank benchmark transactions
 *
 * @author Prajwal
 */
public class SmallBankMessage implements Serializable {

    private static final long serialVersionUID = 7298765432108765432L;

    public enum TransactionType {
        DEPOSIT_CHECKING,
        TRANSACT_SAVINGS,
        WRITE_CHECK,
        SEND_PAYMENT,
        AMALGAMATE,
        BALANCE,
        CREATE_ACCOUNT,
        RESPONSE,
        ERROR
    }

    private TransactionType txType;
    private long customerId;
    private String customerName;
    private long destCustomerId;  // For SendPayment
    private double amount;
    private double savingsBalance;
    private double checkingBalance;
    private int result;
    private String errorMsg;

    private SmallBankMessage() {
        super();
        result = -1;
    }

    // CreateAccount: Creates a new customer account
    public static SmallBankMessage newCreateAccountRequest(long customerId, String customerName,
                                                           double savingsBalance, double checkingBalance) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.CREATE_ACCOUNT;
        message.customerId = customerId;
        message.customerName = customerName;
        message.savingsBalance = savingsBalance;
        message.checkingBalance = checkingBalance;
        return message;
    }

    // DepositChecking: Adds money to checking account
    public static SmallBankMessage newDepositCheckingRequest(long customerId, double amount) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.DEPOSIT_CHECKING;
        message.customerId = customerId;
        message.amount = amount;
        return message;
    }

    // TransactSavings: Deposit or withdraw from savings
    public static SmallBankMessage newTransactSavingsRequest(long customerId, double amount) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.TRANSACT_SAVINGS;
        message.customerId = customerId;
        message.amount = amount;
        return message;
    }

    // WriteCheck: Withdraws money from checking account
    public static SmallBankMessage newWriteCheckRequest(long customerId, double amount) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.WRITE_CHECK;
        message.customerId = customerId;
        message.amount = amount;
        return message;
    }

    // SendPayment: Transfer from one customer's checking to another's
    public static SmallBankMessage newSendPaymentRequest(long sourceCustomerId, long destCustomerId, double amount) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.SEND_PAYMENT;
        message.customerId = sourceCustomerId;
        message.destCustomerId = destCustomerId;
        message.amount = amount;
        return message;
    }

    // Amalgamate: Combine both accounts (move all from checking to savings or vice versa)
    public static SmallBankMessage newAmalgamateRequest(long customerId1, long customerId2) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.AMALGAMATE;
        message.customerId = customerId1;
        message.destCustomerId = customerId2;
        return message;
    }

    public static SmallBankMessage newBalanceRequest(long customerId) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.BALANCE;
        message.customerId = customerId;
        return message;
    }

    // Response message
    public static SmallBankMessage newResponse(int result) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.RESPONSE;
        message.result = result;
        return message;
    }

    // Response with balance information
    public static SmallBankMessage newResponseWithBalances(int result, double savingsBalance, double checkingBalance) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.RESPONSE;
        message.result = result;
        message.savingsBalance = savingsBalance;
        message.checkingBalance = checkingBalance;
        return message;
    }

    // Error message
    public static SmallBankMessage newErrorMessage(String errorMsg) {
        SmallBankMessage message = new SmallBankMessage();
        message.txType = TransactionType.ERROR;
        message.errorMsg = errorMsg;
        return message;
    }

    // Serialize to bytes
    public byte[] getBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this);
            oos.flush();
            baos.flush();
            byte[] bytes = baos.toByteArray();
            oos.close();
            baos.close();
            return bytes;
        } catch (IOException ex) {
            return null;
        }
    }

    // Deserialize from bytes
    public static SmallBankMessage getObject(byte[] bytes) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            SmallBankMessage message = (SmallBankMessage) ois.readObject();
            ois.close();
            bais.close();
            return message;
        } catch (ClassNotFoundException | IOException ex) {
            return null;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SmallBankMessage(type=").append(txType);
        sb.append(", customerId=").append(customerId);
        if (destCustomerId > 0) {
            sb.append(", destCustomerId=").append(destCustomerId);
        }
        if (amount != 0) {
            sb.append(", amount=").append(amount);
        }
        if (customerName != null) {
            sb.append(", customerName=").append(customerName);
        }
        if (savingsBalance != 0) {
            sb.append(", savingsBalance=").append(savingsBalance);
        }
        if (checkingBalance != 0) {
            sb.append(", checkingBalance=").append(checkingBalance);
        }
        sb.append(", result=").append(result);
        sb.append(")");
        return sb.toString();
    }

    // Getters
    public TransactionType getTxType() {
        return txType;
    }

    public long getCustomerId() {
        return customerId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public long getDestCustomerId() {
        return destCustomerId;
    }

    public double getAmount() {
        return amount;
    }

    public double getSavingsBalance() {
        return savingsBalance;
    }

    public double getCheckingBalance() {
        return checkingBalance;
    }

    public int getResult() {
        return result;
    }

    public String getErrorMsg() {
        return errorMsg;
    }
}