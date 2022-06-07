import http from 'k6/http';
export const options = {
  vus: 10,
  duration: '30s',
};
export default function () {
  // http.put('http://localhost:8080/v1/database', `{
  //   "key": "age",
  //   "value": "yo momma"
  // }`);
  http.get('http://localhost:8080/v1/database/age');
}
