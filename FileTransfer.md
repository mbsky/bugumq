# 一对一文件传输 #

一对一文件传输，是基于消息队列Queue实现的，使用的原理是相同的，只不过传输的是二进制数据。

## 方法说明 ##

Client类提供了以下方法，用于一对一文件传输：
```
//设置文件传输监听器
public void setFileListener(FileListener fileListener)

//请求传输文件。注意，这里返回一个long型的fileId
public long requestSendFile(String toClientId, Map<String, String> extras)

//同意接收文件
public void acceptReceiveFile(String toClientId, long fileId, Map<String, String> extras)

//拒绝接收文件
public void rejectReceiveFile(String toClientId, long fileId, Map<String, String> extras)

//发送文件数据
public void sendFileData(long fileId, byte[] data)

//发送文件结束标识符
public void sendEndOfFile(long fileId)
```
其中，FileListener是一个监听器类，它是一个抽象类，包含如下几个抽象方法：
```
//对方请求传输文件
public abstract void onRequest(String fromClientId, long fileId, Map<String,String> extras);

//对方同意接收文件    
public abstract void onAccept(String fromClientId, long fileId, Map<String,String> extras);

//对方拒绝接收文件    
public abstract void onReject(String fromClientId, long fileId, Map<String,String> extras);

//接收到文件数据    
public abstract void onFileData(long fileId, byte[] data);

//接收到文件传输结束的消息    
public abstract void onFileEnd(long fileId);

//文件传输出错
public abstract void onError(long fileId);
```

## 示例代码 ##

发送方：
```
    public void send() throws Exception {
        Client client = Connection.getInstance().getClient();
        client.setFileListener(new SendFileListener());
        
        String toClientId = "receiver_id";
        Map<String, String> extras = new HashMap<String, String>();
        extras.put("filename", "/Users/frankwen/send/redis.pdf");
        //请求传输文件给对方
        client.requestSendFile(toClientId, extras);
    }
    
    class SendFileListener extends FileListener{
        @Override
        public void onRequest(String fromClientId, long fileId, Map<String,String> extras){
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void onAccept(String fromClientId, long fileId, Map<String,String> extras) {
            String filename = extras.get("filename");
            System.out.println(fromClientId + " 同意接收文件：" + filename);
            //开始分块发送文件
            try{
                Client client = Connection.getInstance().getClient();
                FileInputStream fis = new FileInputStream(filename);
                int chunkSize = 16 * 1024;
                byte[] chunkData = new byte[chunkSize];
                int bytesRead = fis.read(chunkData, 0, chunkSize);
                while(bytesRead > 0){
                    if(bytesRead == chunkSize){
                        client.sendFileData(fileId, chunkData);
                    }else{
                        client.sendFileData(fileId, Arrays.copyOfRange(chunkData, 0, bytesRead));
                    }
                    bytesRead = fis.read(chunkData, 0, chunkSize);
                }
                //发送文件结束标识符
                client.sendEndOfFile(fileId);
            }catch(IOException ex){
                
            }
        }

        @Override
        public void onReject(String fromClientId, long fileId, Map<String,String> extras) {
            System.out.println(fromClientId + " 拒绝接收文件：" + extras.get("filename"));
        }

        @Override
        public void onFileData(long fileId, byte[] data) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void onFileEnd(long fileId) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void onError(long fileId){
            throw new UnsupportedOperationException("Not supported yet.");
        }
        
    }
```

接收方：
```
    public void receive() throws Exception {
        Client client = Connection.getInstance().getClient();
        client.setFileListener(new ReceiveFileListener());
    }
    
    class ReceiveFileListener extends FileListener{
        
        private FileOutputStream fos;
        
        @Override
        public void onRequest(String fromClientId, long fileId, Map<String,String> extras){
            String filename = extras.get("filename");
            System.out.println(fromClientId + " 请求向您发送文件：" + filename);
            //发送消息，同意接收文件
            Client client = Connection.getInstance().getClient();
            client.acceptReceiveFile(fromClientId, fileId, extras);

            //新建文件，并打开文件输出流
            try{
                File file = new File("/Users/frankwen/receive/new_file.pdf");
                file.createNewFile();
                fos = new FileOutputStream(file);
            }catch(IOException ex){
                
            }
        }

        @Override
        public void onAccept(String fromClientId, long fileId, Map<String,String> extras) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void onReject(String fromClientId, long fileId, Map<String,String> extras) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void onFileData(long fileId, byte[] data) {
            try{
                fos.write(data);
            }catch(IOException ex){
                
            }
        }

        @Override
        public void onFileEnd(long fileId) {
            System.out.println("文件传输完成！");
            try{
                fos.flush();
                fos.close();
            }catch(IOException ex){
                
            }
        }

        @Override
        public void onError(long fileId){
            System.out.println("文件传输出错！");
            try{
                fos.close();
            }catch(IOException ex){
                
            }
        }
        
    }
```

**注意：**

1、当有多个文件同时在传输的时候，需要用fileId进行区分。fileId是由BuguMQ自动产生的、能唯一区分不同文件的long型数值。

2、BuguMQ按块进行文件传输，块的大小由开发者决定。推荐值是16K字节。